from __future__ import annotations

from typing import Hashable, Protocol, Sequence, TypeVar

from pqdict import pqdict
from tqdm import tqdm

K = TypeVar("K", bound=Hashable)
MAX_DIS = float("inf")


class Node(Protocol):
    id: str
    parents: list[str]
    ancestors: dict[str, int]


def build_ancestors(nodes: Sequence[Node]):
    id2ancestors = get_id2ancestors({node.id: node.parents for node in nodes})
    for node in nodes:
        node.ancestors = id2ancestors[node.id]


def get_id2ancestors(
    id2parents: dict[K, Sequence[K]], verbose: bool = True
) -> dict[K, dict[K, int]]:
    """Create a dictionary mapping from id to ancestors"""
    id2ancestors = {}
    for id in tqdm(id2parents, desc="build ancestors", disable=not verbose):
        id2ancestors[id] = get_dist2ancestors(id, id2parents)
    return id2ancestors


def get_dist2ancestors(id: K, id2parents: dict[K, Sequence[K]]) -> dict[K, int]:
    """Get all ancestors of a given id and the shortest distance to them. Note: distance to the parent is 1.

    Modified of Dijkstra algorithm for nodes that aren't visible until we reach its neighbors.
    """
    dist = {pid: 1 for pid in id2parents[id]}
    pq = pqdict.minpq(dist)
    dist[id] = 0

    while len(pq) > 0:
        pid, pdist = pq.popitem()
        for gid in id2parents[pid]:
            new_dis = pdist + 1
            # if we haven't seen this node before (gid not in dist), we should add it to the queue
            # otherwise, we update its distance if we have a shorter path
            if new_dis < dist.get(gid, MAX_DIS):
                pq[gid] = new_dis
                dist[gid] = new_dis

    dist.pop(id)
    return dist
