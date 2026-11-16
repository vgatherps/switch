"""Wait for requested interrupt affinity to become effective."""
import time


def wait_for_affinity(irqs, cpu, read, timeout=5, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    affinity = {irq: read(irq) for irq in irqs}
    while any(value != cpu for value in affinity.values()) and clock() < deadline:
        sleep(min(0.05, max(0, deadline - clock())))
        affinity = {irq: read(irq) for irq in irqs}
    return affinity
