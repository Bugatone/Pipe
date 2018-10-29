import unittest
from pipe import select, where, concat

from parallelepiped import Parallelepiped


class TestPipes(unittest.TestCase):
    def test_parallelepiped(self):
        par0 = (range(100) | where(lambda x: x % 2 == 1)
                           | select(lambda x: x ** 2)
                           | select(lambda x: x - 1)
                           | where(lambda x: x < 50))

        par1 = (range(100) | where(lambda x: x % 2 == 1)
                           | (Parallelepiped() | select(lambda x: x ** 2))
                           | select(lambda x: x - 1)
                           | where(lambda x: x < 50))

        par2 = (range(100) | where(lambda x: x % 2 == 1)
                           | (Parallelepiped() | select(lambda x: x ** 2)
                                               | select(lambda x: x - 1))
                           | where(lambda x: x < 50))

        l0 = list(par0)
        l1 = list(par1)
        l2 = list(par2)
        self.assertEqual(l0, l1)
        self.assertEqual(l0, l2)


if __name__ == '__main__':
    unittest.main()
