import random
import math
import os

# Stores the vnode to node mapping
# Composed within a node so that every node has its own vnode mapping
class VirtualNodeMap:
    def __init__(self, node_names, TOTAL_VIRTUAL_NODES):
        self._vnode_map = {}
        self._node_names = node_names
        self._TOTAL_VIRTUAL_NODES = TOTAL_VIRTUAL_NODES

    @property
    def vnode_map(self):
        return self._vnode_map

    @property
    def node_names(self):
        return self._node_names

    # Populates the Virtual Node Nap, given the set of Node names.
    # Creates a mapping of Virtual Node to corresponding assigned physical Node
    def populate_map(self):
        # Problem statement 1
        # Generate a dict of vnode ids (0 to (TOTAL_VIRTUAL_NODES - 1) mapped randomly 
        # but equally (as far as maths permits) to node names
        vnode_list = [i for i in range (0, self._TOTAL_VIRTUAL_NODES)]
        random.shuffle(vnode_list)
        for vnode in vnode_list:
            self._vnode_map[vnode] = str(self._node_names[int(vnode % len(self._node_names))])
        self.export_vnode_map_to_file()

    # Return the vnode name mapped to a particular vnode
    def get_node_for_vnode(self, vnode):
        return self._vnode_map[vnode]

    # Returns the vnode name where a particular key is stored
    # It finds the vnode for the key through modulo mapping, and then looks up the physical node
    def get_assigned_node(self, key):
        vnode = key % self._TOTAL_VIRTUAL_NODES
        return self._vnode_map[vnode]

    # Assign a new node name as mapping for a particular vnode
    # This is useful when vnodes are remapped during node addition or removal
    def set_new_assigned_node(self, vnode, new_node_name):
        self._vnode_map[vnode] = new_node_name
        self.export_vnode_map_to_file()


    # Exports the Vnode Map to an external text file 'vnode_map.txt' for later reference after updates
    def export_vnode_map_to_file(self):

        if os.path.exists("vnode_map.txt"):
            os.remove("vnode_map.txt")

        file = open("vnode_map.txt", "w")
        for key in self._vnode_map.keys():
            file.write(str(key) + ":" + " " + str(self._vnode_map[key]))
            file.write("\n")
        file.close()




