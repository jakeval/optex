def topologicalSort(totalVertices, prerequisites):
    ##make graph
    graph = {}
    for edge in prerequisites:
        if edge[0] not in graph:
            graph[edge[0]] = [edge[1]]
        else:
            graph[edge[0]].append(edge[1])
    
    # print(graph)

    n = totalVertices
    indegree = [0]*n
    answer = []
    for key in graph:
        for nbrs in graph[key]:
            indegree[nbrs] += 1
    queue = []
    for i in range(0, n):
        if indegree[i] == 0:
            queue.append((i, 0))
    count = 0
    
    while(len(queue) > 0):
        rem = queue.pop(0)
        answer.append((rem[0], rem[1]))
        if rem[0] in graph:
            for child in graph.get(rem[0]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append((child, rem[1] + 1))
        
    print(answer)
    if count == n:
        return True
    else:
        return False



totalVertices = 5
prereqs = [[1,4],[2,4],[3,1],[3,2]]
print(topologicalSort(totalVertices, prereqs))


def commonLineage(graph1, graph2):
    level1 = checkLevel(graph1)
    level2 = checkLevel(graph2)
    l1 = 0
    l2 = 0
    answer = []
    ptr1 = 0
    ptr2 = 0
    while ptr1 <= level1 or ptr2 <= level2:
        ##in each level, check common elements
        ##add everything from graph1 to a list
        graph1Set = set()
        for i in range(ptr1, len(graph1)):
            if graph1[i][1] == l1:
                graph1Set.add(graph1[i][0])
                ptr1 += 1
        print(graph1Set)
        levelCheck = 0
        for j in range(ptr2, len(graph2)):
            if graph2[j][0] in graph1Set:
                levelCheck+=1
                answer.append(graph2[j][0])
        if levelCheck == 0:
            break
        l1 += 1
        l2 += 1
    print(answer)

def checkLevel(graph):
    levelCount = 0
    for i in range(0, len(graph)-1):
        if graph[i][1] != graph[i+1][1]:
            levelCount += 1
    return levelCount + 1

# graph = [(0, 0), (3, 0), (1, 1), (2, 1), (4, 2), (5, 3)]
# print(checkLevel(graph))

graph1 = [(0, 0), (3, 0), (1, 1), (2, 1), (4, 2), (5, 3)]
graph2 = [(0, 0), (3, 0), (1, 1), (5, 1)]
commonLineage(graph1, graph2)
