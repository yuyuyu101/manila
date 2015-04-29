# Copyright (c) 2012 OpenStack Foundation
# Copyright (c) 2015 Haomai Wang
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os
from oslo_log import log

from manila import exception
from manila.share import driver

try:
    import cephfs
except ImportError:
    cephfs = None


LOG = log.getLogger(__name__)

VirtfsShareOpts = [
    cfg.StrOpt('virtfs_cephfs_conf_path',
               default='',
               help='Path to the cephfs configuration file'),
    cfg.StrOpt('virtfs_cephfs_root_directory',
               default='/',
               help='Path to the cephfs directory which is used by Manila'),
    cfg.StrOpt('virtfs_cephfs_garbage_directory',
               default='/deleted',
               help='Path to the cephfs garbage directory which is used to store'
                     'deleted share'),

]

class CephFSShareDriver(driver.ExecuteMixin, driver.ShareDriver):
    def __init__(self, *args, **kwargs):
        self.backend_name = self.configuration.safe_get(
            'share_backend_name') or 'CephFS'
        self.garbage_dir = self.configuration.virtfs_cephfs_garbage_directory

    def do_setup(self, context):
        """Returns an error if prerequisites aren't met."""
        if cephfs is None:
            msg = _('cephfs python libraries not found')
            raise exception.ShareBackendException(data=msg)
        try:
            with cephfs.LibCephFS(conffile=conf):
                pass
        except cephfs.Error:
            msg = _('error connecting to ceph cluster')
            LOG.exception(msg)
            raise exception.ShareBackendException(data=msg)

    def create_share(self, ctx, share, share_server=None):
        path = os.path.join(self.configuration.virtfs_cephfs_root_directory,
                            share['name'])
        with cephfs.LibCephFS(conffile=conf) as cephfs:
            cephfs.mkdir(path, 0755)
            cephfs.setxattr(path, "ceph.quota.max_bytes", share['size'], 0)
        return path

    def delete_share(self, context, share, share_server=None):
        path = os.path.join(self.configuration.virtfs_cephfs_root_directory,
                            share['name'])
        with cephfs.LibCephFS(conffile=conf) as cephfs:
            cephfs.rename(path, os.path.join(self.garbage_dir, share['name']))

    def create_snapshot(self, context, snapshot, share_server=None):
        """TBD: Is called to create snapshot."""
        raise NotImplementedError()

    def create_share_from_snapshot(self, context, share, snapshot,
                                   share_server=None):
        """Is called to create share from snapshot."""
        raise NotImplementedError()

    def delete_snapshot(self, context, snapshot, share_server=None):
        """TBD: Is called to remove snapshot."""
        raise NotImplementedError()

    def ensure_share(self, context, share, share_server=None):
        """Might not be needed?"""
        pass

    def allow_access(self, context, share, access, share_server=None):
        """Allow access to the share."""
        pass

    def deny_access(self, context, share, access, share_server=None):
        """Deny access to the share."""
        pass
