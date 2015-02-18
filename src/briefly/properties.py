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

import os
import re
import copy
import simplejson

class Properties(object):
  '''A hierachical property collection.
     This class provide direct and convenient access to properties as attributes. For example
     prop = Properties(
       a = 3,
       b = 'x',
       c = '${a}${b}',
       d = Properties(
         e = 4,
       )
     )

     and you can access the properties with:

     prop.a ==> 3
     prop['a'] ==> 3
     prop.b ==> 'x'
     prop.c ==> '3x'
     prop.d.e ==> 4
     prop['d.e'] ==> 4

     All forms can be used as left-value for assignment operator.
     Also a bunch of utilities to manage the properties as stackable configuration.
     TODO: Maybe just inherit from dict?
  '''

  var_pattern = re.compile(r'\$\{([\w\.]+)\}')

  def __init__(self, **kargs):
    '''Initialize property collection'''
    self.__parent__ = None
    self.__data__ = {}
    self.defaults(**kargs)

  def get_copy(self):
    '''Get a clone of this property collection'''
    prop_copy = Properties()
    for key, value in self:
      if isinstance(value, Properties):
        prop_copy[key] = value.get_copy()
      else:
        prop_copy[key] = value
    return prop_copy

  def set_parent(self, parent):
    '''Set the parent of this collection.'''
    self.__parent__ = parent

  def __getattr__(self, name):
    '''Get an attribute, check for system function and variables to avoid
       conflict, and then pass to __getitem__().
    '''
    # Special attributes.
    if name.startswith('__') and name.endswith('__'):
      raise AttributeError(name)

    return self.__getitem__(name)

  def __setattr__(self, name, value):
    '''Set an attribute. Check for system variables and then pass to __setitem__()'''
    if name.startswith('__') and name.endswith('__'):
      self.__dict__[name] = value
    else:
      self.__setitem__(name, value)

  def __getitem__(self, key):
    '''Get an item from collection. Recursively pass down to the final
       collection and then substitute the variables.
    '''

    fields = key.split('.')
    if len(fields) > 1 and self.__data__.has_key(fields[0]):
      assert isinstance(self.__data__[fields[0]], Properties)
      return self.__data__[fields[0]].__getitem__('.'.join(fields[1:]))

    if not self.__data__.has_key(key):
      return None

    node = self
    result = self.__data__[key]

    # deal with the variables from local collection then go up the parent collection.
    while node:
      result = node.sub(result)
      node = node.__parent__
    return result

  def __setitem__(self, key, value):
    '''Set an item in the collection. Pass down to the final collection
       to set the value. Initialize child collection if necessary.
    '''

    fields = key.split('.')
    if len(fields) > 1:
      assert self.__data__.has_key(fields[0]) and isinstance(self.__data__[fields[0]], Properties)
      return self.__data__[fields[0]].__setitem__('.'.join(fields[1:]), value)

    if self.__data__.has_key(key) and self.__data__[key] == value:
      return

    self.__data__[key] = value
    if isinstance(value, Properties):
      value.set_parent(self)

  def __iter__(self):
    '''Iterator of this collection.'''
    return self.__data__.iteritems()

  def __str__(self):
    '''String represention of this collection.'''
    result = []
    for key, value in self:
      result.append('%s = %s' % (key, repr(value)))
    result.sort()
    return '\n'.join(result)

  def __eq__(self, other):
    '''Equality of Properties implementaion.'''
    if self is other:
      return True
    if isinstance(other, Properties):
      return self.get_data() == other.get_data()
    return False

  def __repr__(self):
    '''Representation of this collection.'''
    result = []
    for key, value in self:
      result.append('%s = %s' % (key, repr(value)))
    result.sort()
    return 'Properties(\n  ' + ',\n  '.join(result) + '\n)'

  def __iadd__(self, name, value):
    '''+= operator implementaion.'''
    self.__setattr__(name, self.__getattr__(name) + value)
    
  def get_data(self):
    '''Get the internal dictionary of this collection.'''
    return self.__data__

  def defaults(self, **kargs):
    '''Update the collection only if given entries are not set.'''
    for key, value in kargs.iteritems():
      if not self.__data__.has_key(key):
        if isinstance(value, Properties):
          self.__setitem__(key, value.get_copy())
        else:
          self.__setitem__(key, value)
      elif isinstance(value, Properties):
        assert isinstance(self.__getitem__(key), Properties)
        self.__getitem__(key).defaults(**value.get_data())

    return self

  def set(self, **kargs):
    '''Update the collection unconditionally.'''
    for key, value in kargs.iteritems():
      self.__setitem__(key, value)
    return self

  def sub(self, s, **kargs):
    '''Utility function to substitute variables with value.'''
    if not isinstance(s, basestring):
      if isinstance(s, list):
        # Recursively substitute variables for each list elements.
        return [self.sub(l, **kargs) for l in s]
      else:
        return s

    def lookup(match):
      '''Lookup function for regex substitution.'''
      key = match.group(1)
      if key in kargs:
        value = kargs[key]
      else:
        value = self.__getitem__(key)
      if value is not None:
        return str(value)
      return '${%s}' % key

    return Properties.var_pattern.sub(lookup, s)

  def parse(self, line):
    '''Parse a single line of property definition'''
    fields = line.strip().split('=', 1)
    if len(fields) < 2 or fields[0].startswith('#'):
      return (None, None) # A line of comment
    try:
      key = fields[0].strip()
      try:
        value = simplejson.loads(fields[1].strip())
      except Exception, e:
        # This is to load non json format values as a string.
        value = fields[1].strip()
        # Extend json boolean values to be case-insensitive.
        if value.lower() in ['true', 'false']:
          value = (value.lower() == 'true')
      if not key.startswith('@'): # control commands
        self.__setitem__(key, value)
      return (key, value)
    except Exception, e:
      print "Unable to parse property: ", line
      return (None, None)

  def load(self, filename):
    '''Load a property file'''

    def multiline(pf):
      '''Read and join multiple lines with trailing backslash.'''
      joined_lines = []
      for line in pf:
        line = line.strip()
        if len(line) == 0 or line[0] == '#': # ignore comments
          continue
        if line[-1] == '\\': # join next line?
          joined_lines.append(line[0:-1])
        else:
          yield ''.join(joined_lines) + line
          joined_lines = []

    with open(filename, 'r') as pf:
      for line in multiline(pf):
        key, value = self.parse(line)
        if key == '@import':
          self.load(os.path.join(os.path.dirname(filename), value))
    return self

  def save(self, filename):
    '''Dump properties to a file'''
    with open(filename, 'w') as pf:
      for key, value in self:
        pf.write('%s = %s\n' % (key, simplejson.dumps(value)))
    return self
      
  def to_json(self):
    '''Serialize to json format'''

    def json_dict(d):
      '''Helper to create json friendly dictionary'''
      result = {}
      for key, value in d.iteritems():
        if isinstance(value, Properties):
          result[key] = json_dict(value.get_data())
        else:
          result[key] = value
      return result
    return simplejson.dumps(json_dict(self.get_data()))

  def from_json(self, json_str):
    '''Load from a json string.'''

    def set_dict(prop, d):
      '''Helper to set values from json recursively'''
      for key, value in d.iteritems():
        if isinstance(value, dict):
          if not prop.__data__.has_key(key) or not isinstance(prop[key], Properties):
            prop[key] = Properties()
          set_dict(prop[key], value)
        else:
          prop[key] = value

    d = simplejson.loads(json_str)
    set_dict(self, d)
    return self

  def update(self, other):
    '''Update this instance with another Properties'''
    assert isinstance(other, Properties)
    return self.set(**other.get_data())
