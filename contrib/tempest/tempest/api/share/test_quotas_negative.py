# Copyright 2014 Mirantis Inc.
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

from tempest_lib import exceptions as lib_exc  # noqa
import testtools  # noqa

from tempest.api.share import base
from tempest import test


class SharesQuotasNegativeTest(base.BaseSharesTest):

    force_tenant_isolation = True

    @classmethod
    def resource_setup(cls):
        super(SharesQuotasNegativeTest, cls).resource_setup()

        # Get tenant and user
        cls.identity_client = cls._get_identity_admin_client()
        cls.tenant = cls.identity_client.get_tenant_by_name(
            cls.shares_client.auth_params["tenant"])
        cls.user = cls.identity_client.get_user_by_username(
            cls.tenant["id"], cls.shares_client.auth_params["user"])

    @test.attr(type=["gate", "smoke", "negative"])
    def test_get_quotas_with_empty_tenant_id(self):
        self.assertRaises(lib_exc.NotFound,
                          self.shares_client.show_quotas, "")
