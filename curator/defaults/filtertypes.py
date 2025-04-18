"""Filtertype schema definitions"""

import logging
from curator.defaults import filter_elements, settings

# pylint: disable=missing-docstring, unused-argument, line-too-long


def _age_elements(action, config):
    """
    Sort which filter types that have ``use_age`` are suitable for
    :py:class:`~.curator.IndexList` and which are acceptable in
    :py:class:`~.curator.SnapshotList`, which are required, and which are not.

    :param action: The name of an action
    :type action: str
    :param config: The configuration block for one filter of ``action``
    :type config: dict

    :returns: A :py:class:`list` containing one or more
        :py:class:`~.voluptuous.schema_builder.Optional` or
        :py:class:`~.voluptuous.schema_builder.Required` options from
        :py:mod:`~.curator.defaults.filter_elements`, defining acceptable values
        for each for the given ``action``
    :rtype: list
    """
    retval = []
    is_req = True
    if config['filtertype'] in ['count', 'space']:
        # is_req = True if 'use_age' in config and config['use_age'] else False
        is_req = bool('use_age' in config and config['use_age'])
    retval.append(filter_elements.source(action=action, required=is_req))
    if action in settings.index_actions():
        retval.append(filter_elements.stats_result())
    # This is a silly thing here, because the absence of 'source' will
    # show up in the actual schema check, but it keeps code from breaking here
    ts_req = False
    if 'source' in config:
        if config['source'] == 'name':
            ts_req = True
        elif action in settings.index_actions():
            # field_stats must _only_ exist for Index actions (not Snapshot)
            if config['source'] == 'field_stats':
                retval.append(filter_elements.field(required=True))
            else:
                retval.append(filter_elements.field(required=False))
        retval.append(filter_elements.timestring(required=ts_req))
    else:
        # If source isn't in the config, then the other elements are not
        # required, but should be Optional to prevent false positives
        retval.append(filter_elements.field(required=False))
        retval.append(filter_elements.timestring(required=ts_req))
    return retval


# ### Schema information ###


def alias(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_by_alias`
    """
    return [
        filter_elements.aliases(),
        filter_elements.exclude(),
    ]


def age(action, config):
    """
    :returns: Filter elements acceptable for
        :py:class:`~.curator.IndexList` :py:meth:`~.curator.IndexList.filter_by_age` or
        :py:class:`~.curator.SnapshotList`
        :py:meth:`~.curator.SnapshotList.filter_by_age`
    """
    # Required & Optional
    logger = logging.getLogger('curator.defaults.filtertypes.age')
    retval = [
        filter_elements.direction(),
        filter_elements.unit(),
        filter_elements.unit_count(),
        filter_elements.unit_count_pattern(),
        filter_elements.epoch(),
        filter_elements.exclude(),
    ]
    retval += _age_elements(action, config)
    logger.debug('AGE FILTER = %s', retval)
    return retval


def allocated(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_allocated`
    """
    return [
        filter_elements.key(),
        filter_elements.value(),
        filter_elements.allocation_type(),
        filter_elements.exclude(exclude=True),
    ]


def closed(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_closed`
    """
    return [filter_elements.exclude(exclude=True)]


def count(action, config):
    """
    :returns: Filter elements acceptable for
        :py:class:`~.curator.IndexList` :py:meth:`~.curator.IndexList.filter_by_count`
        or :py:class:`~.curator.SnapshotList`
        :py:meth:`~.curator.SnapshotList.filter_by_count`
    """
    retval = [
        filter_elements.count(),
        filter_elements.use_age(),
        filter_elements.pattern(),
        filter_elements.reverse(),
        filter_elements.exclude(exclude=True),
    ]
    retval += _age_elements(action, config)
    return retval


def empty(action, config):
    """
    :returns: Filter elements acceptable for :py:meth:`~.curator.IndexList.filter_empty`
    """
    return [filter_elements.exclude()]


def forcemerged(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_forceMerged`
    """
    return [
        filter_elements.max_num_segments(),
        filter_elements.exclude(exclude=True),
    ]


def ilm(action, config):
    """
    :returns: Filter elements acceptable for :py:meth:`~.curator.IndexList.filter_ilm`
    """
    return [filter_elements.exclude(exclude=True)]


def kibana(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_kibana`
    """
    return [filter_elements.exclude(exclude=True)]


def none(action, config):
    """
    :returns: Filter elements acceptable for
        :py:class:`~.curator.IndexList` :py:meth:`~.curator.IndexList.filter_none` or
        :py:class:`~.curator.SnapshotList` :py:meth:`~.curator.SnapshotList.filter_none`
    """
    return []


def opened(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_opened`
    """
    return [filter_elements.exclude(exclude=True)]


def pattern(action, config):
    """
    :returns: Filter elements acceptable for
        :py:class:`~.curator.IndexList` :py:meth:`~.curator.IndexList.filter_by_regex`
        or :py:class:`~.curator.SnapshotList`
        :py:meth:`~.curator.SnapshotList.filter_by_regex`
    """
    return [
        filter_elements.kind(),
        filter_elements.value(),
        filter_elements.exclude(),
    ]


def period(action, config):
    """
    :returns: Filter elements acceptable for
        :py:class:`~.curator.IndexList` :py:meth:`~.curator.IndexList.filter_period` or
        :py:class:`~.curator.SnapshotList`
        :py:meth:`~.curator.SnapshotList.filter_period`
    """
    retval = [
        filter_elements.unit(period=True),
        filter_elements.range_from(),
        filter_elements.range_to(),
        filter_elements.week_starts_on(),
        filter_elements.epoch(),
        filter_elements.exclude(),
        filter_elements.period_type(),
        filter_elements.date_from(),
        filter_elements.date_from_format(),
        filter_elements.date_to(),
        filter_elements.date_to_format(),
    ]
    # Only add intersect() to index actions.
    if action in settings.index_actions():
        retval.append(filter_elements.intersect())
    retval += _age_elements(action, config)
    return retval


def shards(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_by_shards`
    """
    return [
        filter_elements.number_of_shards(),
        filter_elements.shard_filter_behavior(),
        filter_elements.exclude(),
    ]


def size(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_by_size`
    """
    return [
        filter_elements.size_threshold(),
        filter_elements.threshold_behavior(),
        filter_elements.size_behavior(),
        filter_elements.exclude(),
    ]


def space(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.IndexList.filter_by_space`
    """
    retval = [
        filter_elements.disk_space(),
        filter_elements.reverse(),
        filter_elements.use_age(),
        filter_elements.exclude(),
        filter_elements.threshold_behavior(),
    ]
    retval += _age_elements(action, config)
    return retval


def state(action, config):
    """
    :returns: Filter elements acceptable for
        :py:meth:`~.curator.SnapshotList.filter_by_state`
    """
    return [
        filter_elements.state(),
        filter_elements.exclude(),
    ]
