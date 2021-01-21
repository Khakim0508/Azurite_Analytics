from collections import deque



def construct_graph(cursor):
    cursor.execute("Select id from Stations;")
    stID = cursor.fetchall()
    graph = {}

    for i in stID:
        cursor.execute("SELECT b, distance FROM routes r where r.a = " + str(i[0]) + ";")
        graph[i[0]] = {}
        if (cursor.rowcount != 0):
            stat = cursor.fetchall()
            for j in stat:
                graph[i[0]][j[0]] = j[1]

    return graph


def bfs(graph, start, end):
    search_queue = deque()
    search_queue += list(graph[start].keys())
    searched = []

    while search_queue:
        curr = search_queue.popleft()

        if (not (curr in searched)):
            if curr == end:
                return True
            else:
                search_queue += list(graph[curr].keys())
                searched.append(curr)

    return False

def dijikstra_graph(graph, start, end):
    infinity = float("inf")
    costs = {}
    for i in graph.keys():
        costs[i] = infinity

    for i in graph[start].keys():
        costs[i] = graph[start][i]


    costs[end] = infinity
    parents = {}
    for i in graph[start].keys():
        parents[i] = start

    parents[end] = None

    processed = [start]

    def find_lowest_cost_node(costs):
        lowest_cost = float("inf")
        lowest_cost_node = None
        for node in costs:
            cost = costs[node]
            if (cost < lowest_cost and node not in processed):
                lowest_cost = cost
                lowest_cost_node = node
        return lowest_cost_node

    node = find_lowest_cost_node(costs)

    while node is not None:
        cost = costs[node]
        neighbors = graph[node]
        for n in neighbors.keys():
            new_cost = cost + neighbors[n]
            if costs[n] > new_cost:
                costs[n] = new_cost
                parents[n] = node
        processed.append(node)
        node = find_lowest_cost_node(costs)

    return parents

