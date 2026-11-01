import unittest

from kernel_irq import wait_for_affinity


class AffinityConvergenceTest(unittest.TestCase):
    def test_waits_until_every_irq_moves(self):
        now = [0.0]
        def read(irq):
            return '31' if irq == '44' or now[0] >= 0.15 else '25'
        def sleep(seconds):
            now[0] += seconds

        actual = wait_for_affinity(['44', '45'], '31', read, timeout=1,
                                   clock=lambda: now[0], sleep=sleep)
        self.assertEqual(actual, {'44': '31', '45': '31'})
        self.assertGreaterEqual(now[0], 0.15)
        self.assertLess(now[0], 1)

    def test_timeout_reports_final_effective_assignments(self):
        now = [0.0]
        def read(irq):
            return '31' if irq == '44' else '25'
        def sleep(seconds):
            now[0] += seconds

        actual = wait_for_affinity(['44', '45'], '31', read, timeout=0.2,
                                   clock=lambda: now[0], sleep=sleep)
        self.assertEqual(actual, {'44': '31', '45': '25'})
        self.assertEqual(now[0], 0.2)


if __name__ == '__main__':
    unittest.main()
