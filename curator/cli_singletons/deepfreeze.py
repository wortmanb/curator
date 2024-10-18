"""Deepfreeze Singleton"""
import click
from curator.cli_singletons.object_class import CLIAction
from datetime import datetime


@click.group("deepfreeze")
def deepfreeze():
    """
    Deepfreeze Singleton
    """
    pass

@deepfreeze.command("rotate")
@click.argument("year", type=int, required=False, default=datetime.now().year)
@click.argument("month", type=int, required=False, default=datetime.now().month)
@click.option(
    "--repo_name_prefix",
    type=str,
    default="deepfreeze-",
    help="prefix for naming rotating repositories",
)
@click.option(
    "--bucket_name_prefix",
    type=str,
    default="deepfreeze-",
    help="prefix for naming buckets",
)
@click.option(
    "--base_path",
    type=str,
    default="snapshots",
    help="base path in the bucket to use for searchable snapshots",
)
@click.option(
    "--canned_acl",
    type=click.Choice(
        [
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "log-delivery-write",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ]
    ),
    default="private",
    help="Canned ACL as defined by AWS",
)
@click.option(
    "--storage_class",
    type=click.Choice(
        [
            "standard",
            "reduced_redundancy",
            "standard_ia",
            "intelligent_tiering",
            "onezone_ia",
        ]
    ),
    default="intelligent_tiering",
    help="What storage class to use, as defined by AWS",
)
@click.option(
    "--keep",
    type=int,
    default=6,
    help="How many repositories should remain mounted?",
)
@click.pass_context
def rotate(
    ctx,
    year,
    month,
    repo_name_prefix,
    bucket_name_prefix,
    base_path,
    canned_acl,
    storage_class,
    keep,
):
    """
    Deepfreeze rotation (add a new repo and age oldest off)
    """
    manual_options = {
        'year': year,
        'month': month,
        'repo_name_prefix': repo_name_prefix,
        'bucket_name_prefix': bucket_name_prefix,
        'base_path': base_path,
        'canned_acl': canned_acl,
        'storage_class': storage_class,
        'keep,   ': keep,
    }
    action = CLIAction(
        ctx.info_name,
        ctx.obj['configdict'],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])

@deepfreeze.command("setup")
@click.argument("year", type=int, required=False, default=datetime.now().year)
@click.argument("month", type=int, required=False, default=datetime.now().month)
@click.option(
    "--repo_name_prefix",
    type=str,
    default="deepfreeze-",
    help="prefix for naming rotating repositories",
)
@click.option(
    "--bucket_name_prefix",
    type=str,
    default="deepfreeze-",
    help="prefix for naming buckets",
)
@click.option(
    "--base_path",
    type=str,
    default="snapshots",
    help="base path in the bucket to use for searchable snapshots",
)
@click.option(
    "--canned_acl",
    type=click.Choice(
        [
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "log-delivery-write",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ]
    ),
    default="private",
    help="Canned ACL as defined by AWS",
)
@click.option(
    "--storage_class",
    type=click.Choice(
        [
            "standard",
            "reduced_redundancy",
            "standard_ia",
            "intelligent_tiering",
            "onezone_ia",
        ]
    ),
    default="intelligent_tiering",
    help="What storage class to use, as defined by AWS",
)
@click.option(
    "--keep",
    type=int,
    default=6,
    help="How many repositories should remain mounted?",
)
@click.pass_context
def setup(
    ctx,
    year,
    month,
    repo_name_prefix,
    bucket_name_prefix,
    base_path,
    canned_acl,
    storage_class,
    keep,
):
    """
    Deepfreeze setup (create the initial repository)
    """
    manual_options = {
        'year': year,
        'month': month,
        'repo_name_prefix': repo_name_prefix,
        'bucket_name_prefix': bucket_name_prefix,
        'base_path': base_path,
        'canned_acl': canned_acl,
        'storage_class': storage_class,
        'keep,   ': keep,
    }
    action = CLIAction(
        ctx.info_name,
        ctx.obj['configdict'],
        manual_options,
        [],
        True,
    )
    action.do_singleton_action(dry_run=ctx.obj['dry_run'])
