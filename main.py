#!/usr/bin/env python3
"""Exploration benchmark comparing four static coordination configurations plus a dynamic ring.

Research prototype: simplified goal-conflict verification/enforcement proxy, NOT a faithful
implementation of formal VERIFYAC/ENFORCEAC. Sequential/parallel describe message scheduling
in this discrete-time model, not actual distributed hardware execution.
"""
import argparse, csv, json, math, random, statistics, time, os
from concurrent.futures import ProcessPoolExecutor
from collections import deque
from pathlib import Path

UNKNOWN, FREE, WALL = -1, 0, 1
CONFIGS = ["all_to_all_sequential", "all_to_all_parallel", "multi_leader_ring_sequential", "multi_leader_ring_parallel", "dynamic_ring"]

def nbrs(p,w,h):
    x,y=p
    for q in ((x+1,y),(x-1,y),(x,y+1),(x,y-1)):
        if 0<=q[0]<w and 0<=q[1]<h: yield q

def md(a,b): return abs(a[0]-b[0])+abs(a[1]-b[1])

def pathfind(start,goal,known,w,h):
    if start==goal:return [start]
    q=deque([start]); prev={start:None}
    while q:
        p=q.popleft()        
        for n in nbrs(p,w,h):
            if n in prev or known.get(n,UNKNOWN)!=FREE: continue
            prev[n]=p
            if n==goal:
                out=[]; cur=n
                while cur is not None: out.append(cur); cur=prev[cur]
                return out[::-1]
            q.append(n)
    return []

# Worker-process state: each process gets a read-only copy of the world once.
_MOVE_WORLD = None
_MOVE_W = None
_MOVE_H = None

def _init_move_worker(world, width, height):
    global _MOVE_WORLD, _MOVE_W, _MOVE_H
    _MOVE_WORLD, _MOVE_W, _MOVE_H = world, width, height

def _parallel_move_one(task):
    """Plan and apply one robot's movement inside a separate worker process."""
    r, step, seed = task
    rng = random.Random(seed + step * 1000003 + r["id"] * 9176)
    if not r["alive"] or r["battery"] <= 0:
        return r

    def robot_frontiers(robot):
        return [
            p for p, v in robot["known"].items()
            if v == FREE and any(
                n not in robot["known"] for n in nbrs(p, _MOVE_W, _MOVE_H)
            )
        ]

    fs = robot_frontiers(r)
    if r["goal"] is None or r["goal"] not in fs:
        if not fs:
            r["goal"] = None
        else:
            dist = min(md(r["pos"], p) for p in fs)
            near = [p for p in fs if md(r["pos"], p) == dist]
            # Per-robot deterministic RNG avoids sharing a random generator
            # between processes.
            r["goal"] = rng.choice(near)

    if r["goal"] is None:
        return r

    path = pathfind(r["pos"], r["goal"], r["known"], _MOVE_W, _MOVE_H)
    if len(path) > 1:
        r["pos"] = path[1]
        r["battery"] = max(0, r["battery"] - .0007)
        r["visited"].add(r["pos"])
        x, y = r["pos"]
        for xx in range(max(0, x-1), min(_MOVE_W, x+2)):
            for yy in range(max(0, y-1), min(_MOVE_H, y+2)):
                p = (xx, yy)
                r["known"][p] = _MOVE_WORLD[p]
        r["known"][r["pos"]] = FREE
    elif r["pos"] == r["goal"]:
        x, y = r["pos"]
        for xx in range(max(0, x-1), min(_MOVE_W, x+2)):
            for yy in range(max(0, y-1), min(_MOVE_H, y+2)):
                p = (xx, yy)
                r["known"][p] = _MOVE_WORLD[p]
        r["known"][r["pos"]] = FREE
        fs = robot_frontiers(r)
        r["goal"] = rng.choice(fs) if fs else None
    return r

class Sim:
    def __init__(self, name, robots=12, clusters=3, steps=180, seed=17, width=30, height=30):
        self.name=name; self.n=robots; self.k=clusters; self.steps=steps; self.rng=random.Random(seed); self.seed=seed
        self.w=width; self.h=height; self.radio=8.0; self.loss=.04; self.maxlat=3
        self.world={(x,y):FREE for x in range(width) for y in range(height)}
        cells=[(x,y) for x in range(2,width-2) for y in range(2,height-2) if abs(x-width//2)+abs(y-height//2)>5]
        self.rng.shuffle(cells)
        for p in cells[:int(width*height*.12)]: self.world[p]=WALL
        free=[p for p,v in self.world.items() if v==FREE]; self.rng.shuffle(free)
        self.rs=[]
        for i in range(robots):
            p=free[i]; self.rs.append(dict(id=i,pos=p,battery=1.0,alive=True,cluster=i%clusters,goal=None,known={p:FREE},visited={p},rel=.9,leader_since=0,better=0))
            self.observe(self.rs[-1])
        self.leaders={c:min(r['id'] for r in self.rs if r['cluster']==c) for c in range(clusters)}
        self.links={}; self.prev_edges=set(); self.pending=[]; self.msg=0; self.deliv=0; self.drop=0; self.bytes=0
        self.leader_changes=0; self.member_changes=0; self.conflict_ticks=0; self.version=0; self.rows=[]; self.send_count_step=0
        self.rebuild_links()

    def observe(self,r):
        x,y=r['pos']
        for xx in range(max(0,x-1),min(self.w,x+2)):
            for yy in range(max(0,y-1),min(self.h,y+2)):
                p=(xx,yy); r['known'][p]=self.world[p]
        r['known'][r['pos']]=FREE

    def rebuild_links(self):
        links={}; active=[r for r in self.rs if r['alive'] and r['battery']>0]
        for i,a in enumerate(active):
            for b in active[i+1:]:
                d=math.dist(a['pos'],b['pos'])
                if d<=self.radio: links[tuple(sorted((a['id'],b['id'])))]=max(.05,1-.65*d/self.radio)
        edges=set(links)
        if edges!=self.prev_edges:self.version+=1
        self.prev_edges=edges; self.links=links

    def q(self,a,b): return self.links.get(tuple(sorted((a,b))),0)
    def members(self,c): return [r['id'] for r in self.rs if r['alive'] and r['battery']>0 and r['cluster']==c]

    def allowed_pairs(self):
        active=[r['id'] for r in self.rs if r['alive'] and r['battery']>0]; pairs=set()
        if self.name.startswith('all_to_all'):
            for i,a in enumerate(active):
                for b in active[i+1:]:
                    if self.q(a,b)>0:pairs.add(tuple(sorted((a,b))))
        else:
            # Cluster member-to-leader links plus a ring among leaders; physical range still applies.
            for r in self.rs:
                if not r['alive'] or r['battery']<=0:continue
                l=self.leaders.get(r['cluster'],r['id'])
                if l!=r['id'] and self.q(l,r['id'])>0:pairs.add(tuple(sorted((l,r['id']))))
            cs=sorted(c for c,l in self.leaders.items() if l in active)
            if len(cs)>1:
                for i,c in enumerate(cs):
                    a=self.leaders[c]; b=self.leaders[cs[(i+1)%len(cs)]]
                    if a!=b and self.q(a,b)>0:pairs.add(tuple(sorted((a,b))))
        return sorted(pairs)

    def send(self,t,s,r,kind,payload):
        if self.q(s,r)<=0:return
        self.msg+=1; self.bytes+=64; self.rs[s]['battery']=max(0,self.rs[s]['battery']-.00004)
        # Explicit service model: sequential schedulers serialize sends in batches of 8;
        # parallel schedulers submit the tick's messages as a batch.
        service_delay=(self.send_count_step//8) if self.name.endswith('sequential') else 0
        self.send_count_step+=1
        loss=min(.95,self.loss+(1-self.q(s,r))*.5)
        if self.rng.random()<loss:
            self.drop+=1; self.rs[s]['rel']=.97*self.rs[s]['rel']; return
        self.pending.append((t+self.rng.randint(1,self.maxlat)+service_delay,s,r,kind,payload))
        self.rs[s]['rel']=.97*self.rs[s]['rel']+.03

    def deliver(self,t):
        ready=[m for m in self.pending if m[0]<=t]; self.pending=[m for m in self.pending if m[0]>t]
        if self.name.endswith('sequential'):ready.sort(key=lambda m:(m[0],m[1],m[2]))
        for _,s,r,kind,payload in ready:
            if not self.rs[s]['alive'] or not self.rs[r]['alive'] or self.q(s,r)<=0:self.drop+=1;continue
            self.deliv+=1
            if kind=='map':self.rs[r]['known'].update(payload)
            elif kind=='enforce':self.rs[r]['goal']=payload

    def frontiers(self,r):
        return [p for p,v in r['known'].items() if v==FREE and any(n not in r['known'] for n in nbrs(p,self.w,self.h))]

    def choose_goal(self,r):
        fs=self.frontiers(r)
        if not fs:return None
        # Deliberately use only local knowledge here: robots do not inspect the
        # global goal table. Duplicate goals are possible and must be resolved by
        # communication/coordination rather than a simulator-wide oracle.
        d=min(md(r['pos'],p) for p in fs); near=[p for p in fs if md(r['pos'],p)==d]
        return self.rng.choice(near)

    def move(self, pool=None, step=0):
        if pool is not None:
            tasks = [(r, step, self.seed) for r in self.rs]
            # Each robot's planning/movement runs in a worker process.
            updated = list(pool.map(_parallel_move_one, tasks))
            self.rs = updated
            return

        # Sequential baseline: same movement logic, executed in this process.
        for r in self.rs:
            if not r['alive'] or r['battery']<=0:continue
            if r['goal'] is None or r['goal'] not in self.frontiers(r):r['goal']=self.choose_goal(r)
            if r['goal'] is None:continue
            path=pathfind(r['pos'],r['goal'],r['known'],self.w,self.h)
            if len(path)>1:
                r['pos']=path[1]; r['battery']=max(0,r['battery']-.0007); r['visited'].add(r['pos']); self.observe(r)
            elif r['pos']==r['goal']:
                self.observe(r); r['goal']=self.choose_goal(r)

    def score(self,rid,c):
        r=self.rs[rid]; ms=self.members(c); others=[i for i in ms if i!=rid]
        conn=sum(self.q(rid,i)>0 for i in others)/len(others) if others else 1
        load=len(ms)/self.n
        return .45*conn+.35*r['rel']+.20*r['battery']-.15*load

    def dynamic_update(self,t):
        if self.name!='dynamic_ring':return
        if t>0 and t%12==0:
            for r in self.rs:
                if not r['alive'] or r['battery']<=0 or self.leaders.get(r['cluster'])==r['id']:continue
                c=r['cluster']; lid=self.leaders.get(c,r['id']); cur=self.q(r['id'],lid)-.15*len(self.members(c))/self.n
                opts=[]
                for nc,nl in self.leaders.items():
                    if nc!=c and self.rs[nl]['alive'] and self.rs[nl]['battery']>0 and self.q(r['id'],nl)>0:
                        opts.append((self.q(r['id'],nl)-.15*len(self.members(nc))/self.n,nc))
                if opts:
                    u,nc=max(opts)
                    if u>=cur+.08:r['cluster']=nc;self.member_changes+=1;self.version+=1
        for c in list(self.leaders):
            ms=self.members(c)
            if not ms:continue
            cur=self.leaders[c]; eligible=[i for i in ms if self.rs[i]['battery']>.08]
            if not eligible:continue
            if cur not in eligible:
                new=max(eligible,key=lambda i:self.score(i,c))
                if new!=cur:self.leaders[c]=new;self.rs[new]['leader_since']=t;self.leader_changes+=1;self.version+=1
                continue
            challengers=[i for i in eligible if i!=cur]
            if not challengers:continue
            ch=max(challengers,key=lambda i:self.score(i,c)); imp=self.score(ch,c)-self.score(cur,c)
            self.rs[cur]['better']=self.rs[cur]['better']+1 if imp>=.12 else 0
            if t-self.rs[cur]['leader_since']>=10 and self.rs[cur]['better']>=3:
                self.leaders[c]=ch;self.rs[ch]['leader_since']=t;self.rs[cur]['better']=0;self.leader_changes+=1;self.version+=1

    def coordinate(self,t):
        pairs=self.allowed_pairs(); conflict=False
        for a,b in pairs:
            ra,rb=self.rs[a],self.rs[b]
            # Exchange small local-map summaries; sequential/parallel only changes service ordering.
            self.send(t,a,b,'map',dict(list(ra['known'].items())[:20]));self.send(t,b,a,'map',dict(list(rb['known'].items())[:20]))
            if ra['cluster']==rb['cluster'] and ra['goal'] is not None and ra['goal']==rb['goal']:
                conflict=True
                leader=self.leaders.get(ra['cluster'],a); follower=b if leader==a else a
                fr=self.rs[follower]; alts=[p for p in self.frontiers(fr) if p!=fr['goal']]
                if alts:
                    ng=min(alts,key=lambda p:md(fr['pos'],p));self.send(t,leader,follower,'enforce',ng)
        return conflict

    def run(self):
        total_free=sum(v==FREE for v in self.world.values())
        pool = None
        if self.name.endswith("parallel"):
            workers = min(self.n, os.cpu_count() or 1)
            pool = ProcessPoolExecutor(
                max_workers=workers,
                initializer=_init_move_worker,
                initargs=(self.world, self.w, self.h)
            )
        for t in range(self.steps):
            # Random temporary robot faults/recovery make the network dynamic for every config.
            for r in self.rs:
                if r['alive'] and self.rng.random()<.0005:r['alive']=False
                elif not r['alive'] and self.rng.random()<.08:r['alive']=True;r['battery']=max(r['battery'],.25)
            self.rebuild_links();self.deliver(t);self.dynamic_update(t);self.move(pool, t);conf=self.coordinate(t)
            if conf:self.conflict_ticks+=1
            observed=set()
            for r in self.rs:observed.update(p for p,v in r['known'].items() if v==FREE)
            coverage=len(observed & {p for p,v in self.world.items() if v==FREE})/total_free
            self.rows.append({'step':t,'config':self.name,'coverage':coverage,'active_robots':sum(r['alive'] and r['battery']>0 for r in self.rs),
              'network_edges':len(self.links),'messages_sent':self.msg,'messages_delivered':self.deliv,'messages_dropped':self.drop,'bytes_sent':self.bytes,
              'leader_changes':self.leader_changes,'membership_changes':self.member_changes,'cluster_sizes':json.dumps({str(c):sum(r['cluster']==c and r['alive'] for r in self.rs) for c in range(self.k)},sort_keys=True),
              'topology_version':self.version,'goal_conflict_tick':int(conf),'mean_battery':statistics.mean(r['battery'] for r in self.rs)})
        if pool is not None:
            pool.shutdown()
        cov=[r['coverage'] for r in self.rows]; last=self.rows[-1]
        return {'config':self.name,'seed':self.seed,'robots':self.n,'clusters':self.k,'steps':self.steps,'final_coverage':last['coverage'],
          'mean_coverage':statistics.mean(cov),'messages_sent':self.msg,'messages_delivered':self.deliv,'messages_dropped':self.drop,'bytes_sent':self.bytes,
          'leader_changes':self.leader_changes,'membership_changes':self.member_changes,'goal_conflict_tick_rate':self.conflict_ticks/self.steps,
          'mean_active_robots':statistics.mean(r['active_robots'] for r in self.rows),'mean_network_edges':statistics.mean(r['network_edges'] for r in self.rows),
          'final_cluster_sizes':last['cluster_sizes'],'caveat':'Simplified goal-conflict/enforcement proxy; not formal VERIFYAC/ENFORCEAC.'}

def main():
    p=argparse.ArgumentParser();p.add_argument('--robots',type=int,default=12);p.add_argument('--clusters',type=int,default=3);p.add_argument('--steps',type=int,default=180);p.add_argument('--seed',type=int,default=17);p.add_argument('--outdir',default='mr_ac_experiment_results');a=p.parse_args()
    if a.robots<3 or not 1<=a.clusters<=a.robots or a.steps<1:p.error('Require robots>=3, 1<=clusters<=robots, steps>=1')
    out=Path(a.outdir);out.mkdir(parents=True,exist_ok=True); summaries=[]; rows=[]
    for name in CONFIGS:
        sim=Sim(name,a.robots,a.clusters,a.steps,a.seed)
        start_time=time.perf_counter()
        result=sim.run()
        result['execution_time_seconds']=time.perf_counter()-start_time
        summaries.append(result)
        rows.extend(sim.rows)
    (out/'summary.json').write_text(json.dumps(summaries,indent=2),encoding='utf-8')
    with (out/'timeseries.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    print(f'robots={a.robots}, clusters={a.clusters}, steps={a.steps}, seed={a.seed}')
    print(f"{'configuration':31s} {'coverage':>9s} {'messages':>10s} {'delivered':>10s} {'leader sw.':>10s} {'membership':>11s} {'runtime (s)':>12s}")
    for s in summaries:print(f"{s['config']:31s} {s['final_coverage']:9.3f} {s['messages_sent']:10d} {s['messages_delivered']:10d} {s['leader_changes']:10d} {s['membership_changes']:11d} {s['execution_time_seconds']:12.4f}")
    print('Wrote',out/'summary.json','and',out/'timeseries.csv')
if __name__=='__main__':main()