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

from briefly.dag import *
from briefly.properties import *
import unittest

class TestGraphNode(unittest.TestCase):
  myattrs = {'attr' : 1}

  def test_no_attrs(self):
    graph_node = GraphNode()
    self.assertTrue(hasattr(graph_node, 'parents'))
    self.assertTrue(hasattr(graph_node, 'children'))
    self.assertTrue(graph_node.attributes is None)
    dummy_key = 1
    key = graph_node.unpack(dummy_key)
    self.assertTrue(key == dummy_key)

  def test_attrs(self):
    graph_node = GraphNode(self.myattrs)
    self.assertTrue(len(graph_node.attributes) == 1)
    self.assertTrue(graph_node.attributes['attr'] == 1)

  def test_int_attrs(self):
    metadata = 3
    graph_node = GraphNode(metadata)
    self.assertTrue( graph_node.attributes == metadata)
    dummy_key = 1
    key, attrs = graph_node.unpack(dummy_key)
    self.assertTrue(attrs == metadata)
    self.assertTrue(key == dummy_key)

  def test_attr_err(self):
    graph_node = GraphNode()
    expected = False
    try:
      graph_node.parents = set([2])
    except AttributeError:
      expected = True
    self.assertTrue(expected)


class TestCaseIslands(unittest.TestCase):
  '''  Only islands in the graph. '''
  def setUp(self):
    self.nodes = set([1,2,3])
    self.g = DependencyGraph()
    self.g.add_node(1)
    self.g.add_node(2)
    self.g.add_node(3)
    self.g.add_edge(1,1)

  def test_nodes(self):
    wait = False
    self.assertTrue(len(self.g) == 3)
    for n in self.nodes:
      self.assertTrue(n in self.g)
    for n in self.g.nodes:
      self.assertTrue(n in self.nodes)
    self.g.remove(2)
    self.assertTrue(1 in self.g)
    self.assertTrue(3 in self.g)
    self.assertFalse(2 in self.g)
    self.assertFalse(4 in self.g)
    self.assertTrue(len(self.g) == 2)
    self.assertFalse(self.g.is_empty())
    self.g.remove(1)
    self.g.remove(3)
    self.assertTrue(self.g.is_empty())


class TestCaseSanity(unittest.TestCase):
  ''' Graph
      1 -> 2 -> 3 -> 4 -> 5 -> 6
      sequence should be 1, 2, 3, 4, 5, 6
  '''
  def setUp(self):
    self.edges = [(x,x+1) for x in xrange(1,6)]              
    self.g = DependencyGraph()
    for p,c in [(x,x+1) for x in xrange(1,6)]:
      self.g.add_node(p)
      self.g.add_node(c)
    self.g.add_edges(self.edges)
    repr(self.g)

  def _test_bridge_nodes(self, iterable):
    for x in iterable:
      bridge_nodes = list(self.g.get_bridge_nodes(x))
      self.assertTrue(len(bridge_nodes) == 1)
      self.assertTrue(bridge_nodes[0] == x+1)

  def _test_node_exists(self, iterable):
    for x in iterable:
      self.assertTrue(x in self.g)

  def test_nodes(self):
    self._test_node_exists(xrange(1,7))
    for x in xrange(1,6):
      self.assertTrue(self.g.has_child(x))
    for x in xrange(2,7):
      self.assertTrue(self.g.has_parent(x))
    for p, c in self.edges:
      self.assertTrue(self.g.has_edge(p, c))
    for key in self.g:
      self.assertTrue(self.g.has_node(key))

  def test_bridge_nodes(self):
    self._test_bridge_nodes(xrange(1,5))
    self.g.remove

  def test_remove(self):
    for s in xrange(1, 5):
      self.assertTrue(s in self.g)
      self.g.remove(s)
      self.assertFalse(s in self.g)
      # make sure other nodes are still there.  
      self._test_node_exists(xrange(s+1,7))
      self._test_bridge_nodes(xrange(s+1,5))

  def test_remove_middle(self):
    self.assertTrue(2 in self.g)
    self.g.remove(2)
    self.assertFalse(2 in self.g)
    self.assertTrue(1 in self.g)
    self.assertFalse(self.g.has_child(1))
    for n in xrange(3,7):
      self.assertTrue(n in self.g)
    for x in xrange(3,6):
      self.assertTrue(self.g.has_child(x))


class TestCaseParallel(unittest.TestCase):
  '''  Graph
       1 -> 2
       3 -> 4
       5 -> 6
  '''
  def setUp(self):
    self.edges = [(x,x+1) for x in xrange(1,6,2)]
    self.g = DependencyGraph()
    for p,c in [(x,x+1) for x in xrange(1,6,2)]:
      self.g.add_node(p)
      self.g.add_node(c)
    self.g.add_edges(self.edges)

  def test_start_nodes(self):
    start_nodes = set(self.g.get_start_nodes())
    self.assertTrue(len(start_nodes)==3)
    for x in xrange(1,6,2):
      self.assertTrue(x in start_nodes)

  def test_bridge_nodes(self):
    # bridges nodes of 1,3,5 are 2,4,6 respectively.
    for x in xrange(1,6,2):
      bnodes = list(self.g.get_bridge_nodes(x))
      self.assertTrue(len(bnodes)==1)
      self.assertTrue(x+1 == bnodes[0])
    # no bridge nodes for 2,4,6
    for x in xrange(2,7,2):
      bnodes = list(self.g.get_bridge_nodes(x))
      self.assertTrue(len(bnodes)==0)


class TestAttributeGraph(unittest.TestCase):
  '''  Graph
       1 -> 2
  '''
  def setUp(self):
    self.g = DependencyGraph()
    self.g.add_node(1, {'name':1})
    self.g.add_node(2, {'name':2})
    self.g.add_edge(1, 2)

  def test_attribute(self):
    for n, attr in self.g.nodes:
      self.assertTrue(attr['name'] == n)

  def test_start_nodes(self):
    start_nodes = list(self.g.get_start_nodes())
    self.assertTrue(len(start_nodes)==1)
    node, attr = start_nodes[0]
    self.assertTrue(node == 1)
    self.assertTrue(node == 1)

  def test_bridge_nodes(self):
    bridge_nodes = list(self.g.get_bridge_nodes(1))
    self.assertTrue(len(bridge_nodes)==1)
    node, attr = bridge_nodes[0]
    self.assertTrue(node== 2)
    self.assertTrue(attr['name'] == 2)


class TestPropertiesGraph(unittest.TestCase):
  '''  Graph
       1 -> 2
  '''
  def setUp(self):
    self.g = DependencyGraph()
    self.g.add_node(1, Properties( name = 1 ))
    self.g.add_node(2, Properties( name = 2 ))
    self.g.add_edge(1, 2)

  def test_properties(self):
    for n, prop in self.g.nodes:
      self.assertTrue(prop.name == n)

  def test_bridge_nodes(self):
    bridge_nodes = list(self.g.get_bridge_nodes(1))
    self.assertTrue(len(bridge_nodes)==1)
    node, prop = bridge_nodes[0]
    self.assertTrue(prop.name == 2)


if __name__ == '__main__':
  unittest.main()
