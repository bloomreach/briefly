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

from briefly.properties import *
import unittest

class TestProperties(unittest.TestCase):
  ''' Test case for class Properties. '''
  def setUp(self):
    ''' Test case setup. '''
    self.prop  = Properties(
                      a = '${b}',
                      b = 'x',
                      c = '${a}${b}',
                      d = Properties(
                        e = 4,
                      ),
                      f = '${variable_non_exist}',
                      g = '',
                      h = '${g}',
                      i = ['${a}', '${b}', '${c}'],
                      )
    self.answers = self.prop.get_data()

  def test_equality(self):
    ''' Test equality protocol. '''
    c = self.prop.get_copy()
    self.assertEqual(self.prop, self.prop)
    self.assertEqual(self.prop, c)
    self.assertEqual(str(self.prop), str(c))
    self.assertFalse(self.prop == 1)

  def test_private(self):
    ''' Test private attributes. '''
    self.assertRaises(AttributeError, getattr, self.prop, '__test__')

  def test_setitem(self):
    ''' Test setitem method '''
    self.prop['g'] = 'new'
    self.assertEqual(self.prop.g, 'new')
    self.prop.g = 'new2'
    self.assertEqual(self.prop.g, 'new2')

  def test_fields(self):
    ''' Test attributes get/set. '''
    self.assertEqual(self.prop.d.e, 4)
    self.assertEqual(self.prop['d.e'], 4)
    self.prop['d.e'] = 4
    self.assertEqual(self.prop.d.e, 4)
    self.assertEqual(self.prop['d.e'], 4)
    self.prop['d.e'] = 5
    self.assertEqual(self.prop.d.e, 5)
    self.assertEqual(self.prop['d.e'], 5)

  def test_repr(self):
    ''' Test repr protocol. '''
    self.assertEqual(eval(repr(self.prop)), self.prop)

  def test_set(self):
    ''' Test set method. '''
    self.prop.set(a = 'foo', b = 'bar')
    self.assertEqual(self.prop.a, 'foo')
    self.assertEqual(self.prop.b, 'bar')

  def test_sub(self):
    ''' Test variable substitution. '''
    self.assertEqual(self.prop.a, 'x')
    self.assertEqual(self.prop.b, 'x')
    self.assertEqual(self.prop.c, 'xx')
    self.assertEqual(self.prop.f, '${variable_non_exist}')
    self.assertEqual(self.prop.sub('${a}'), 'x')
    self.assertEqual(self.prop.sub('${f}', f=5), '5')
    self.assertEqual(self.prop.i, [self.prop.a, self.prop.b, self.prop.c])

  def test_defaults(self):
    ''' Make sure that only non-existing config can be updated. '''
    self.prop.defaults(a='new_value')
    self.assertEqual(self.prop.a, 'x')
    self.prop.defaults(foo='foo')
    self.assertEqual(self.prop.foo, 'foo')
    self.prop.defaults(value=Properties(value='value'))
    self.assertEqual(self.prop.value, Properties(value='value'))
    
  def test_parse(self):
    ''' Test parse method. '''
    self.assertEqual(self.prop.parse('#conf = value'), (None, None))
    self.assertEqual(self.prop.parse('conf ! value'), (None, None))
    self.assertEqual(self.prop.parse('conf = value'), ('conf', 'value'))

  def test_json(self):
    ''' Test json related methods. '''
    self.assertEqual(self.prop.from_json(self.prop.to_json()), self.prop)

  def test_builtin_false(self):
    ''' Test values that evalates to False in boolean. '''
    self.assertEqual(self.prop.h, '')

  def test_boolean_values(self):
    ''' Test case-insensitive boolean values. '''
    self.assertEqual(self.prop.parse('bv = TRUE'), ('bv', True))
    self.assertEqual(self.prop.parse('bv = True'), ('bv', True))
    self.assertEqual(self.prop.parse('bv = true'), ('bv', True))
    self.assertEqual(self.prop.parse('bv = FALSE'), ('bv', False))
    self.assertEqual(self.prop.parse('bv = False'), ('bv', False))
    self.assertEqual(self.prop.parse('bv = false'), ('bv', False))

  def test_update(self):
    ''' Test update method. '''
    self.prop.update(Properties(foo='foo'))
    self.assertEqual(self.prop.foo, 'foo')
    self.prop.update(Properties(d=Properties(e=6)))
    self.assertEqual(self.prop['d.e'], 6)

  def test_iadd(self):
    ''' Test += operator. '''
    self.prop.i += ['${a}']
    self.assertEqual(self.prop.i[-1], self.prop.a)
    self.prop.c += '${a}'
    self.assertEqual(self.prop.c, 'xxx')

  def test_nonexist_vars(self):
    '''Test non-exist variable substitution.'''
    self.prop.ne = '${nosuch_var}'
    self.assertEqual(str(self.prop.ne), '${nosuch_var}')
    self.prop.ne = '${nosuch_var.nosuch_member}'
    self.assertEqual(str(self.prop.ne), '${nosuch_var.nosuch_member}')
    self.prop.ne = '${nosuch_var.nosuch_member.nosuch_child}'
    self.assertEqual(str(self.prop.ne), '${nosuch_var.nosuch_member.nosuch_child}')


if __name__ == '__main__':
  unittest.main()
