from collections import defaultdict, deque
import pandas as pd

# === 入力（IDはinputsに依存） ===
items = [
  {"ID":"A001","inputs":["C001"]},
  {"ID":"A002","inputs":["A001"]},
  {"ID":"A003","inputs":["A002","B001"]},
  {"ID":"A004","inputs":["A003","B003"]},
  {"ID":"B000","inputs":[]},
  {"ID":"B001","inputs":["A001","C001","B000"]},
  {"ID":"B002","inputs":["A002","B001","C002"]},
  {"ID":"B003","inputs":["B002","C003"]},
  {"ID":"B004","inputs":["B003"]},
  {"ID":"B005","inputs":["A004","B004","C004"]},
  {"ID":"C001","inputs":[]},
  {"ID":"C002","inputs":["B001","C001","B000"]},
  {"ID":"C003","inputs":["A003","B001","C002"]},
  {"ID":"C004","inputs":["C003"]},
]

# === グラフ構築（input -> ID） ===
adj = defaultdict(list)
indegree = defaultdict(int)
nodes = set()
for row in items:
    nodes.add(row["ID"])
for row in items:
    nid = row["ID"]
    for src in row["inputs"]:
        nodes.add(src)
        adj[src].append(nid)
        indegree[nid] += 1
for n in nodes:
    adj[n] = adj.get(n, [])
    indegree[n] = indegree.get(n, 0)

sources = sorted([n for n in nodes if indegree[n] == 0])
sinks   = sorted([n for n in nodes if len(adj[n]) == 0])

# === DAG検証（Kahn） ===
in_deg = {n:0 for n in nodes}
for u in nodes:
    for v in adj[u]:
        in_deg[v] += 1
q = deque([n for n,d in in_deg.items() if d==0])
topo = []
while q:
    u = q.popleft()
    topo.append(u)
    for v in adj[u]:
        in_deg[v]-=1
        if in_deg[v]==0:
            q.append(v)
assert len(topo) == len(nodes), "Cycle detected: not a DAG."

# === 全パス列挙（sources -> sinks） ===
def enumerate_paths(adj, sources, sinks):
    sink_set = set(sinks)
    out = []
    def dfs(u, path):
        path.append(u)
        if u in sink_set:
            out.append(path.copy())
        else:
            for v in adj[u]:
                dfs(v, path)
        path.pop()
    for s in sources:
        dfs(s, [])
    return out

paths = enumerate_paths(adj, sources, sinks)

# === 画面出力（整形） ===
lines = [f"{i:02d}. " + " → ".join(p) for i, p in enumerate(paths, start=1)]
print("\n".join(lines))
print(f"\nTotal: {len(paths)} paths  | sources: {sources} | sinks: {sinks}")

# === テキスト保存（任意） ===
with open("all_paths_list.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

# === CSV（正規化/サマリー、任意） ===
rows = []
for pid, p in enumerate(paths, start=1):
    for step, n in enumerate(p, start=1):
        rows.append({"path_id": pid, "step": step, "node": n, "source": p[0], "sink": p[-1]})
df_steps = pd.DataFrame(rows)
df_summary = (df_steps.groupby('path_id')
              .agg(source=('source','first'), sink=('sink','first'), length=('step','max'))
              .reset_index())
df_summary['path'] = df_steps.groupby('path_id')['node'].apply(list).values
df_steps.to_csv("all_paths_steps.csv", index=False)
df_summary.to_csv("all_paths_summary.csv", index=False)
