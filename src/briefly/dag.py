#
# Copyright 2013-2015 BloomReach, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from properties import Properties

class GraphNode(object):
  ''' Internal graph node data strcuture used by this module. '''

  def __init__(self, attributes=None, **kwargs):
    ''' self.parents and self.children cannot be reassigned by mistake. '''
    self.__dict__['parents'] = set()
    self.__dict__['children'] = set()
    self.attributes = attributes
    self.set_attributes(attributes, **kwargs)

  def __setattr__(self, name, value):
    ''' Override setattr to control the access. '''
    if name in ('parents', 'children'):
      raise AttributeError("%s is an immutable attribute." % (name,))
    super(GraphNode, self).__setattr__(name, value)
    
  def set_attributes(self, attributes={}, **kwargs):
    ''' Setting the attributes. '''
    if isinstance(self.attributes, dict):
      self.attributes.update(attributes)
      self.attributes.update(kwargs)
    else:
      self.attributes = attributes

  def unpack(self, node_key):
    ''' Unpack the node into (node_key, attributes) if attributes were provided.
        Or simply return node_key if attributes were not provided previously.
    '''
    if isinstance(self.attributes, dict):
      if self.attributes:
        return node_key, self.attributes
      else:
        return node_key
    if self.attributes is not None:
      return node_key, self.attributes
    return node_key
    
  def __repr__(self):
    ''' Implement repr() protocol.'''
    parents_str = ','.join(map(repr, self.parents))
    children_str = ','.join(map(repr, self.children))
    return '(p: %s, c: %s)' % (parents_str, 
                               children_str)

class DependencyGraph(object):
  ''' Directed acylic graph.

      Note that the data structure does not impose synchronization.
      The client has to synchronize the access of the graph.
  '''
  def __init__(self):
    ''' self.node_map represents { node_key : GraphNode }. '''
    self.clear()

  def clear(self):
    ''' Clear the entire graph.'''
    self.node_map = {}

  def __contains__(self, node):
    ''' Implment 'in' protocol.'''
    return self.has_node(node)

  def __iter__(self):
    ''' Implment iterator protocol.'''
    return self.node_map.iterkeys()

  def __len__(self):
    ''' Implment len() protocol.'''
    return len(self.node_map)

  def __repr__(self):
    ''' Implement repr() protocol.'''
    return repr(self.node_map)

  @property
  def nodes(self):
    ''' Return all nodes in the graph.'''
    nodes = []
    for k, graph_node in self.node_map.iteritems():
      nodes.append(graph_node.unpack(k))
    return nodes

  def add_edges(self, edges):
    ''' Add edges into graph. '''
    for edge in edges:
      self.add_edge(*edge)
  
  def add_edge(self, parent, child):
    ''' Add an edge  parent -> child.'''
    if parent is child:
      # degenerate case, do nothing.
      assert parent in self.node_map, 'node %s not existed.' % (repr(parent),)
      return
    
    assert parent in self.node_map, 'node %s not existed.' % (repr(parent),)
    assert child in self.node_map, 'node %s not existed.' % (repr(child),)

    self.get_children(parent).add(child)
    self.get_parents(child).add(parent)
    
  def add_node(self, node_key, attrs_or_property=None, **argd):
    ''' Add a node into the graph. '''
    self.node_map.setdefault(node_key,
                             GraphNode(attrs_or_property, **argd))

  def get_node_set(self, node_key, relation='parents'):
    ''' Common methods called by get_parents and get_children. '''
    graph_node = self.node_map.get(node_key)
    return getattr(graph_node, relation)

  def get_parents(self, node_key):
    ''' Get a set of parents node keys from node_key.'''
    return self.get_node_set(node_key, relation='parents')

  def get_children(self, node_key):
    ''' Get a set of children node keys from node_key.'''
    return self.get_node_set(node_key, relation='children')

  def get_start_nodes(self):
    ''' Get start nodes (those nodes do not have incoming edges) from the graph. '''
    nodes = []
    for node_key, graph_node in self.node_map.iteritems():
      if not self.has_parent(node_key):
        nodes.append(graph_node.unpack(node_key))
    return nodes

  def get_bridge_nodes(self, node_key):
    ''' Get bridge nodes give a node_key (those nodes have node_key as their the only parent).
        If node_key is removed the graph, bridge nodes will be the next 'start nodes'.
    '''
    nodes = []
    for c in self.get_children(node_key):
      parents = self.get_parents(c)
      if len(parents) == 1 and node_key in parents:
        graph_node = self.node_map[c]
        nodes.append(graph_node.unpack(c))
    return nodes

  def remove(self, node_key):
    ''' Remove a node in the graph. Throws an exception if node does not exists.'''

    for p in self.get_parents(node_key):
      self.get_children(p).remove(node_key)

    for c in self.get_children(node_key):
      self.get_parents(c).remove(node_key)

    if node_key in self.node_map:
      self.node_map.pop(node_key)
    
  def has_parent(self, node_key):
    ''' Determine if a node has parent(s).'''
    return len(self.get_parents(node_key)) > 0

  def has_child(self, node_key):
    ''' Determine if a node has child(ren).'''
    return len(self.get_children(node_key)) > 0

  def has_node(self, node_key):
    ''' Check if the graph has the node_key.'''
    return node_key in self.node_map

  def has_edge(self, parent, child):
    ''' Check if the graph has an edge parent -> child.'''

    assert self.has_node(parent)
    assert self.has_node(child)

    has_cinp = (child in self.get_children(parent))
    has_pinc = (parent in self.get_parents(child))
    
    assert has_cinp == has_pinc
    return has_cinp

  def is_empty(self):
    ''' Check if the graph containing no nodes.'''
    return len(self.node_map) == 0
