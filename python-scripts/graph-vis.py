import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz as pgv
from networkx.drawing.nx_agraph import graphviz_layout

#G.graph_attr['nodesep'] = 2.0

networkSize = 20
breadth = 2

cursor = 1


def wire(graph):
    next_line = [0]
    global cursor

    while cursor < networkSize:
        next_line = handleNextLevel(graph, next_line)


def handleNextLevel(graph, current_line):
    global cursor
    next_line = []

    while len(current_line) > 0:
        counter = 0
        current_branch = current_line.pop(0)
        while counter < breadth:
            if cursor == networkSize:
                return []
            counter += 1
            graph.add_edge(current_branch, cursor)
            graph.add_edge(cursor, current_branch)
            next_line.append(cursor)
            cursor += 1
    return next_line


#G = nx.DiGraph(nodesep=20.0)
#G = pgv.AGraph(nodesep='2.0', ranksep='10.0')
G = pgv.AGraph()
G.add_nodes_from(range(0, networkSize))

#for x in range(0, networkSize) :
    #n = G.get_node(x).atrr['nodesep'] = 20.0


wire(G)

#pos = graphviz_layout(G, prog='dot')
#pos = nx.spring_layout(G, k = 500)
#nx.draw(G, pos, with_labels=True, arrows=False)

#plt.title('draw_networkx')
#pos =graphviz_layout(G, prog='dot')
#nx.draw(G, with_labels=True, font_weight='bold')
#plt.show()
#plt.savefig("Graph2.png", format="PNG")
G.layout(prog='dot')
G.draw('Graph.png')