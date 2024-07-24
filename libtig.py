import argparse
import collections
import configparser
from datetime import datetime
from genericpath import isdir
import grp, pwd
from fnmatch import fnmatch
import hashlib
from math import ceil
import os
import re
import sys
import zlib


class GitRepository(object):
    """A git repository"""
    worktree = None
    gitdir = None
    config = None

    def __init__(self, path, force=False) -> None:
        self.worktree = path
        self.gitdir = os.path.join(path, ".git")

        if not(force or os.path.isdir(self.gitdir)):
            raise Exception(f"Not a git repository {path}")
        
        # Read config file in .git/config
        self.config = configparser.ConfigParser()
        cf = repo_file(self, "config")

        if cf and os.path.exists(cf):
            self.config.read([cf])

        elif not force:
            raise Exception("Config file missing!")
        
        if not force:
            vers = int(self.config.get("core", "repositoryformatversion"))
            if vers != 0:
                raise Exception(f"Unsupported repositoryformatversion {vers}")
    
def repo_path(repo, *path):
    """Compute path under repo's gitdir"""
    return os.path.join(repo.gitdir, *path)
    
def repo_dir(repo, *path, mkdir=False):
    """Same as repo_path, but mkdir *path if absent"""
    path = repo_path(repo, *path)
    if os.path.exists(path):
        if os.path.isdir(path):
            return path
        else:
            raise Exception(f"Not a directory {path}")
    
    if mkdir:
        os.makedirs(path)
        return path
    else:
        return None
    
def repo_file(repo, *path, mkdir=False):
    """Same as repo_path, but create dirname(*path) if absent.  For
    example, repo_file(r, \"refs\", \"remotes\", \"origin\", \"HEAD\") will create
    .git/refs/remotes/origin."""
    if repo_dir(repo, *path[:-1], mkdir=mkdir):
        return repo_path(repo, *path)
    
def repo_create(path):
    """Create new repository at path"""
    repo = GitRepository(path, True)

    # First, we make sure the path either doesn't exist or is an
    # empty dir.

    if os.path.exists(repo.worktree):
        if not os.path.isdir(repo.worktree):
            raise Exception(f"{path} not a directory!")
        if os.path.exists(repo.gitdir) and os.listdir(repo.gitdir):
            raise Exception(f"{path} is not empty!")
    else:
        os.makedirs(repo.worktree) 

    assert repo_dir(repo, "branches", mkdir=True)
    assert repo_dir(repo, "objects", mkdir=True)
    assert repo_dir(repo, "refs", "tags", mkdir=True)
    assert repo_dir(repo, "refs", "heads", mkdir=True)

    # .git/description
    with open(repo_file(repo, "description"), "w") as file:
        file.write("Unnamed repository; edit this file 'description' to name the repository.\n")
    
     # .git/HEAD
    with open(repo_file(repo, "HEAD"), "w") as file:
        file.write("ref: refs/heads/master\n")

    with open(repo_file(repo, "config"), "w") as file:
        config = repo_default_config()
        config.write(file)

    return repo

def repo_default_config():
    ret = configparser.ConfigParser()
    ret.add_section("core")
    ret.set("core", "repositoryformatversion", "0")
    ret.set("core", "filemode", "false")
    ret.set("core", "bare", "false")

    return ret

def repo_find(path=".", required=True):
    path = os.path.realpath(path)

    if os.path.isdir(os.path.join(path, ".dir")):
        return GitRepository(path)
    parent = os.path.realpath(os.path.join(path, ".."))

    if parent == path:
        # If parent == path, parent is root

        if required:
            raise Exception("No git directory!")
        else:
            return None
        
    return repo_find(parent, required)


def cmd_init(args):
    repo_create(args.path)

argparser = argparse.ArgumentParser(description="Simple content tracker")
argsubparsers = argparser.add_subparsers(title="Commands", dest="command")
argsp = argsubparsers.add_parser("init", help="Initialize a new, empty repository.")
argsp.add_argument("path", metavar="directory", nargs="?", default=".", help="Where to create the repository.")
argsubparsers.required = True

def main(argv=sys.argv[1:]):
    args = argparser.parse_args(argv)
    match args.command:
        case "add"          : cmd_add(args)
        case "cat-file"     : cmd_cat_file(args)
        case "check-ignore" : cmd_check_ignore(args)
        case "checkout"     : cmd_checkout(args)
        case "commit"       : cmd_commit(args)
        case "hash-object"  : cmd_hash_object(args)
        case "init"         : cmd_init(args)
        case "log"          : cmd_log(args)
        case "ls-files"     : cmd_ls_files(args)
        case "ls-tree"      : cmd_ls_tree(args)
        case "rev-parse"    : cmd_rev_parse(args)
        case "rm"           : cmd_rm(args)
        case "show-ref"     : cmd_show_ref(args)
        case "status"       : cmd_status(args)
        case "tag"          : cmd_tag(args)
        case _              : print("Bad command.")


