#!/usr/bin/python3
class fvar :

     def __init__(self, variable, value=None) :
         print('constructing ...'):u
         self.value = value
         self.variable = variable
         print (f'constructing: {self.variable}'
     @property

     def variable(self) :
         print('getting ...')
         return(lambda : self.update(self._value))
     @variable.setter

     def variable(self, variable) :
         print ('Setting ...')
         self._variable = variable
         if (self.value != None) :
             self._value = self.value

     def __call__(self, value=None) :
         print( 'calling ...')
         if (value != None) :
             self.value = value
         return(self.value)

     def update(self, value=None) :
         print ('updating ...')
         if (value != None) :
             self.value = value
         return(self.value)
