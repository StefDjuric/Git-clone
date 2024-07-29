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
            

class GitObject(object):
    """This is an abstract class"""

    def __init__(self, data=None) -> None:
        if data != None:
            self.deserialize(data)
        else:
            self.init()

    def deserialize(self, data):
        raise Exception("Unimplemented!")
    
    def serialize(self, repo):
        raise Exception("Unimplemented!")
    
    def init(self, data):
        pass


class GitBlob(GitObject):
    fmt = b'blob'

    def serialize(self):
        return self.blobdata

    def deserialize(self, data):
        self.blobdata = data



def object_read(repo, sha):
    """Read object sha from Git repository repo.  Return a
    GitObject whose exact type depends on the object."""

    path = repo_file(repo, "objects", sha[0:2], sha[2:])

    if not os.path.isfile(path):
        return None
    
    with open(path, "rb")as file:
        raw = zlib.decompress(file.read())

        # read object type
        x = raw.find(b' ')
        fmt = raw[0 : x]

        # read and validate object size
        y = raw.find(b'\x00', x)
        size = int(raw[x : y].decode("ascii"))

        if size != len(raw) - y - 1:
            raise Exception("Malformed object {0}: bad length".format(sha))
        
        # pick constructor
        match fmt:
            case b'commit' : constructor=GitCommit
            case b'tree'   : constructor=GitTree
            case b'tag'    : constructor=GitTag
            case b'blob'   : constructor=GitBlob
            case _: raise Exception("Unknown type {0} for object {1}".format(fmt.decode("ascii"), sha))

        return constructor(raw[y + 1:])
    
def object_write(obj: GitObject, repo=None):

    data = obj.serialize()
    
    # add header
    result = obj.fmt + b' ' + str(len(data)).encode() + b'\x00' + data

    # compute hash
    sha = hashlib.sha1(result).hexdigest()

    if repo:
        # compute path
        path = repo_file(repo, "objects", sha[0:2], sha[2:], mkdir=True)

        if not os.path.exists(path):
            with open(path, "wb") as file:
                # compress and write
                file.write(zlib.compress(result))
    return sha

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


def object_find(repo, name, fmt=None, follow=True):
    """Name resolution function"""
    return name


def cat_file(repo, object, fmt=None):
    obj = object_read(repo, object_find(repo, obj, fmt=fmt))
    sys.stdout.buffer.write(obj.serialize())

def object_hash(file, fmt, repo=None):
    """Hash object, writing it to repo if provided"""
    data = file.read()

    # Choose constructor
    match fmt:
        case b'commit' : obj=GitCommit(data)
        case b'tree'   : obj=GitTree(data)
        case b'tag'    : obj=GitTag(data)
        case b'blob'   : obj=GitBlob(data)
        case _: raise Exception(f"Unknown type {fmt}!")

    return object_write(obj, repo)


def kvlm_parse(raw, start=0, dct=None):
    if not dct:
        dct = collections.OrderedDict()

    # This function is recursive: it reads a key/value pair, then call
    # itself back with the new position.  So we first need to know
    # where we are: at a keyword, or already in the messageQ

    # We search for the next space and the next newline.
    spc = raw.find(b' ', start)
    nl = raw.find(b'\n', start)

    # If space appears before newline, we have a keyword.  Otherwise,
    # it's the final message, which we just read to the end of the file.

    # Base case
    # =========
    # If newline appears first (or there's no space at all, in which
    # case find returns -1), we assume a blank line.  A blank line
    # means the remainder of the data is the message.  We store it in
    # the dictionary, with None as the key, and return.
    if (spc < 0) or  (nl < spc):
        assert nl == start
        dct[None] = raw[start+1:]
        return dct
    
    # we read a key-value pair and recurse for the next.
    key = raw[start:spc]

    # Find the end of the value.  Continuation lines begin with a
    # space, so we loop until we find a "\n" not followed by a space.
    end = start
    while True:
        end = raw.find(b'\n', end + 1)
        if raw[end + 1] != ord(' '): 
            break
    
    # Grab the value
    # Also, drop the leading space on continuation lines
    value = raw[spc+1:end].replace(b'\n ', b'\n')

    # Don't overwrite existing data contents
    if key in dct:
        if type(dct[key]) == list:
            dct[key].append(value)
        else:
            dct[key] = [dct[key], value]
    else:
        dct[key] = value

    return kvlm_parse(raw,start=end + 1, dct=dct)

def kvlm_serialize(kvlm):
    ret = b''

    # Output fields
    for key in kvlm.keys():

        # Skip the message
        if key == None:
            continue

        value = kvlm[key]
        # Normalize to a list
        if type(value) != list:
            value = [value]

        for val in value:
            ret += key + b' ' + (val.replace(b'\n', b'\n ')) + b'\n'

        # Append message   
        ret += b'\n' + kvlm[None] + b'\n'

        return ret


def cmd_init(args):
    repo_create(args.path)



def cmd_cat_file(args):
    repo = repo_find()
    cat_file(repo, args.object, fmt=args.type.encode())

def cmd_hash_object(args):
    if args.write:
        repo = repo_find()
    else:
        repo = None
    
    with open(args.path, "rb") as file:
        sha = object_hash(file, args.type.encode(), repo)
        print(sha)



# ARGS READING 
argparser = argparse.ArgumentParser(description="Simple content tracker")
argsubparsers = argparser.add_subparsers(title="Commands", dest="command")
argsp = argsubparsers.add_parser("init", help="Initialize a new, empty repository.")
argsp = argsubparsers.add_parser("cat-file", help="Provide content of repository objects")
argsp = argsubparsers.add_parser("hash-object", help="Compute object ID and optionally creates a blob from a file")
argsp.add_argument("type", metavar="type", choices=["blob", "commit", "tag", "tree"], help="Specify the type")
argsp.add_argument("object", metavar="object", help="object to display")
argsp.add_argument("path", metavar="directory", nargs="?", default=".", help="Where to create the repository.")
argsp.add_argument("-t", metavar="type", dest="type", choices=["blob", "commit", "tag", "tree"], default="blob", help="specify the type")
argsp.add_argument("-w", dest="write", action="store_true", help="Actually write the object into database")
argsp.add_argument("path", help="Read object from file")
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


