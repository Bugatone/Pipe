import unittest
from pipe import Parallelepiped, select, where, concat


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

    def test_right_or(self):
        ror_piped = (range(100) | where(lambda x: x % 2 == 1)
                                | select(lambda x: x ** 2)
                                | select(lambda x: x - 1)
                                | where(lambda x: x < 50))

        or_pipe = (where(lambda x: x % 2 == 1) | select(lambda x: x ** 2)
                                               | select(lambda x: x - 1)
                                               | where(lambda x: x < 50))

        lror = list(ror_piped)
        lor = list(range(100) | or_pipe)
        self.assertEqual(lror, lor)

if __name__ == '__main__':
    unittest.main()
