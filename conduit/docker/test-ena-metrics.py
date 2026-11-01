#!/usr/bin/env python3
"""Exercise patched ENA metrics callbacks with a fake admin queue.

Usage: python3 test-ena-metrics.py /path/to/unmodified/dpdk-25.11.3
Requires a C compiler and patch; does not require DPDK libraries or hardware.
"""

import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile


PRELUDE = r'''
#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define ENA_ADMIN_CUSTOMER_METRICS 1
#define ENA_ADMIN_ENI_STATS 2
#define ENA_ADMIN_CUSTOMER_METRICS_SUPPORT_MASK 0x3f
#define ENA_STATS_ARRAY_GLOBAL 1
#define ENA_STATS_ARRAY_METRICS 6
#define ENA_STATS_ARRAY_METRICS_LEGACY 5
#define ENA_STATS_ARRAY_ENA_SRD 1
#define ENA_STATS_ARRAY_RX 1
#define ENA_STATS_ARRAY_TX 1
#define ENA_MAX_CUSTOMER_METRICS 6
#define PMD_DRV_LOG_LINE(...) ((void)0)
#define rte_spinlock_lock(lock) ((void)(lock))
#define rte_spinlock_unlock(lock) ((void)(lock))
#define ENA_PROXY(adapter, fn, ...) fn(__VA_ARGS__)

struct ena_com_dev {
    unsigned int caps;
    struct { uint64_t supported_metrics; } customer_metrics;
};
struct ena_admin_eni_stats { uint64_t values[5]; };
struct ena_stats_srd { uint64_t value; };
struct ring { uint64_t rx_stats, tx_stats; };
struct ena_adapter {
    struct ena_com_dev ena_dev;
    int admin_lock;
    unsigned int metrics_num;
    uint64_t dev_stats, srd_stats;
    struct ring rx_ring[1], tx_ring[1];
};
struct rte_eth_dev_data {
    void *dev_private;
    unsigned int nb_rx_queues, nb_tx_queues;
};
struct rte_eth_dev { struct rte_eth_dev_data *data; };
struct rte_eth_xstat { uint64_t id, value; };
struct stat_desc { int stat_offset; };
static const struct stat_desc ena_stats_global_strings[] = {{0}};
static const struct stat_desc ena_stats_metrics_strings[] = {
    {0}, {8}, {16}, {24}, {32}, {40}
};
static const struct stat_desc ena_stats_srd_strings[] = {{0}};
static const struct stat_desc ena_stats_rx_strings[] = {{0}};
static const struct stat_desc ena_stats_tx_strings[] = {{0}};
static int query_result, query_calls;
static bool ena_com_get_cap(struct ena_com_dev *dev, unsigned int cap)
{
    return (dev->caps & cap) != 0;
}
static int ena_com_get_customer_metrics(struct ena_com_dev *dev, char *buf, size_t len)
{
    uint64_t values[6] = {11, 12, 13, 14, 15, 16};
    (void)dev;
    ++query_calls;
    if (!query_result)
        memcpy(buf, values, len);
    return query_result;
}
static int ena_com_get_eni_stats(struct ena_com_dev *dev, struct ena_admin_eni_stats *stats)
{
    uint64_t values[5] = {11, 12, 13, 14, 15};
    (void)dev;
    ++query_calls;
    if (!query_result)
        memcpy(stats, values, sizeof(values));
    return query_result;
}
static void ena_copy_ena_srd_info(struct ena_adapter *adapter, struct ena_stats_srd *srd)
{
    (void)adapter;
    (void)srd;
}
static unsigned int ena_xstats_calc_num(struct rte_eth_dev_data *data)
{
    struct ena_adapter *adapter = data->dev_private;
    return 2 + adapter->metrics_num + data->nb_rx_queues + data->nb_tx_queues;
}
'''

TEST = r'''
static void expect_callbacks(struct rte_eth_dev *dev, int expected)
{
    struct rte_eth_xstat all[64];
    uint64_t ids[] = {1, 2}, values[] = {0, 0};
    assert(ena_xstats_get(dev, all, 64) == expected);
    assert(ena_xstats_get_by_id(dev, ids, values, 2) == expected);
}
int main(void)
{
    struct ena_adapter adapter = {0};
    struct rte_eth_dev_data data = {.dev_private = &adapter};
    struct rte_eth_dev dev = {.data = &data};
    struct rte_eth_xstat all[64];
    uint64_t ids[] = {1, 2}, values[2];
    uint64_t global_id = 0, global_value;

    adapter.ena_dev.caps = ENA_ADMIN_CUSTOMER_METRICS;
    adapter.ena_dev.customer_metrics.supported_metrics = 0x3f;
    adapter.metrics_num = 6;
    query_result = -EIO;
    expect_callbacks(&dev, -EIO);
    assert(query_calls == 2);
    query_result = 0;
    query_calls = 0;
    assert(ena_xstats_get(&dev, all, 64) == 8);
    assert(all[1].value == 11 && all[6].value == 16);
    assert(ena_xstats_get_by_id(&dev, ids, values, 2) == 2);
    assert(values[0] == 11 && values[1] == 12 && query_calls == 2);

    adapter.ena_dev.customer_metrics.supported_metrics = 0x1f;
    expect_callbacks(&dev, -ENOTSUP);
    adapter.ena_dev.customer_metrics.supported_metrics = 0x3e;
    expect_callbacks(&dev, -ENOTSUP);
    adapter.ena_dev.customer_metrics.supported_metrics = 0;
    expect_callbacks(&dev, -ENOTSUP);
    assert(query_calls == 2);
    assert(ena_xstats_get_by_id(&dev, &global_id, &global_value, 1) == 1);
    adapter.ena_dev.customer_metrics.supported_metrics = 0x3f;
    adapter.metrics_num = 5;
    expect_callbacks(&dev, -EINVAL);

    adapter.ena_dev.caps = ENA_ADMIN_ENI_STATS;
    query_result = -ETIMEDOUT;
    expect_callbacks(&dev, -ETIMEDOUT);
    query_result = 0;
    assert(ena_xstats_get(&dev, all, 64) == 7 && all[5].value == 15);
    assert(ena_xstats_get_by_id(&dev, ids, values, 2) == 2 && values[1] == 12);
    adapter.metrics_num = 4;
    expect_callbacks(&dev, -EINVAL);

    adapter.ena_dev.caps = 0;
    expect_callbacks(&dev, -EINVAL);
    adapter.metrics_num = 0;
    query_calls = 0;
    assert(ena_xstats_get(&dev, all, 64) == 2);
    assert(ena_xstats_get_by_id(&dev, &global_id, &global_value, 1) == 1);
    assert(query_calls == 0);
    return 0;
}
'''


def extract(source, name):
    match = re.search(r"static int\s+" + name + r"\s*\([^;{}]*\)\s*\{", source)
    if not match:
        raise ValueError(f"cannot find definition of {name}")
    depth = 1
    end = match.end()
    while depth:
        if source[end] == "{":
            depth += 1
        elif source[end] == "}":
            depth -= 1
        end += 1
    return source[match.start():end]


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    source = Path(sys.argv[1]).resolve()
    if (source / "VERSION").read_text().strip() != "25.11.3":
        raise SystemExit("Expected unmodified DPDK 25.11.3 source")
    patch = Path(__file__).resolve().with_name("dpdk-ena-read-clock.patch")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name in ("base/ena_com.c", "base/ena_plat_dpdk.h", "ena_ethdev.c"):
            rel = Path("drivers/net/ena") / name
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / rel, root / rel)
        subprocess.run(["patch", "-s", "-p1", "-i", str(patch)], cwd=root, check=True)
        ethdev = (root / "drivers/net/ena/ena_ethdev.c").read_text()
        functions = ("ena_copy_customer_metrics", "ena_xstats_get", "ena_xstats_get_by_id")
        harness = root / "test.c"
        harness.write_text(PRELUDE + "\n".join(extract(ethdev, name) for name in functions) + TEST)
        binary = root / "test"
        subprocess.run(shlex.split(os.environ.get("CC", "cc")) + ["-std=c11", str(harness), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True)


if __name__ == "__main__":
    main()
