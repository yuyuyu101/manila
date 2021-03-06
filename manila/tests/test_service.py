# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2014 NetApp, Inc.
# Copyright 2014 Mirantis, Inc.
#
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

"""
Unit Tests for remote procedure calls using queue
"""

import mock
from oslo_config import cfg

from manila import context
from manila import db
from manila import exception
from manila import manager
from manila import service
from manila import test
from manila import wsgi

test_service_opts = [
    cfg.StrOpt("fake_manager",
               default="manila.tests.test_service.FakeManager",
               help="Manager for testing"),
    cfg.StrOpt("test_service_listen",
               default=None,
               help="Host to bind test service to"),
    cfg.IntOpt("test_service_listen_port",
               default=0,
               help="Port number to bind test service to"),
]

CONF = cfg.CONF
CONF.register_opts(test_service_opts)


class FakeManager(manager.Manager):
    """Fake manager for tests."""

    RPC_API_VERSION = "1.0"

    def __init__(self, host=None, db_driver=None, service_name=None):
        super(FakeManager, self).__init__(host=host, db_driver=db_driver)

    def test_method(self):
        return 'manager'


class ExtendedService(service.Service):
    def test_method(self):
        return 'service'


class ServiceManagerTestCase(test.TestCase):
    """Test cases for Services."""

    def test_message_gets_to_manager(self):
        serv = service.Service('test', 'test', 'test', CONF.fake_manager)
        serv.start()
        self.assertEqual(serv.test_method(), 'manager')

    def test_override_manager_method(self):
        serv = ExtendedService('test', 'test', 'test', CONF.fake_manager)
        serv.start()
        self.assertEqual(serv.test_method(), 'service')


class ServiceFlagsTestCase(test.TestCase):
    def test_service_enabled_on_create_based_on_flag(self):
        self.flags(enable_new_services=True)
        host = 'foo'
        binary = 'manila-fake'
        app = service.Service.create(host=host, binary=binary)
        app.start()
        app.stop()
        ref = db.service_get(context.get_admin_context(), app.service_id)
        db.service_destroy(context.get_admin_context(), app.service_id)
        self.assertFalse(ref['disabled'])

    def test_service_disabled_on_create_based_on_flag(self):
        self.flags(enable_new_services=False)
        host = 'foo'
        binary = 'manila-fake'
        app = service.Service.create(host=host, binary=binary)
        app.start()
        app.stop()
        ref = db.service_get(context.get_admin_context(), app.service_id)
        db.service_destroy(context.get_admin_context(), app.service_id)
        self.assertTrue(ref['disabled'])


def fake_service_get_by_args(*args, **kwargs):
    raise exception.NotFound()


def fake_service_get(*args, **kwargs):
    raise Exception()


host = 'foo'
binary = 'bar'
topic = 'test'
service_create = {
    'host': host,
    'binary': binary,
    'topic': topic,
    'report_count': 0,
    'availability_zone': 'nova',
}
service_ref = {
    'host': host,
    'binary': binary,
    'topic': topic,
    'report_count': 0,
    'availability_zone': 'nova',
    'id': 1,
}


class ServiceTestCase(test.TestCase):
    """Test cases for Services."""

    def test_create(self):
        app = service.Service.create(host='foo',
                                     binary='manila-fake',
                                     topic='fake')
        self.assertTrue(app)

    @mock.patch.object(service.db, 'service_get_by_args',
                       mock.Mock(side_effect=fake_service_get_by_args))
    @mock.patch.object(service.db, 'service_create',
                       mock.Mock(return_value=service_ref))
    @mock.patch.object(service.db, 'service_get',
                       mock.Mock(side_effect=fake_service_get))
    def test_report_state_newly_disconnected(self):
        serv = service.Service(host, binary, topic, CONF.fake_manager)
        serv.start()
        serv.report_state()
        self.assertTrue(serv.model_disconnected)
        service.db.service_get_by_args.assert_called_once_with(
            mock.ANY, host, binary)
        service.db.service_create.assert_called_once_with(
            mock.ANY, service_create)
        service.db.service_get.assert_called_once_with(mock.ANY, mock.ANY)

    @mock.patch.object(service.db, 'service_get_by_args',
                       mock.Mock(side_effect=fake_service_get_by_args))
    @mock.patch.object(service.db, 'service_create',
                       mock.Mock(return_value=service_ref))
    @mock.patch.object(service.db, 'service_get',
                       mock.Mock(return_value=service_ref))
    @mock.patch.object(service.db, 'service_update',
                       mock.Mock(return_value=service_ref.
                                 update({'report_count': 1})))
    def test_report_state_newly_connected(self):
        serv = service.Service(host, binary, topic, CONF.fake_manager)
        serv.start()
        serv.model_disconnected = True
        serv.report_state()
        self.assertFalse(serv.model_disconnected)
        service.db.service_get_by_args.assert_called_once_with(
            mock.ANY, host, binary)
        service.db.service_create.assert_called_once_with(
            mock.ANY, service_create)
        service.db.service_get.assert_called_once_with(
            mock.ANY, service_ref['id'])
        service.db.service_update.assert_called_once_with(
            mock.ANY, service_ref['id'], mock.ANY)


class TestWSGIService(test.TestCase):

    @mock.patch.object(wsgi.Loader, 'load_app', mock.Mock())
    def test_service_random_port(self):
        test_service = service.WSGIService("test_service")
        self.assertEqual(0, test_service.port)
        test_service.start()
        self.assertNotEqual(0, test_service.port)
        test_service.stop()
        wsgi.Loader.load_app.assert_called_once_with("test_service")


class TestLauncher(test.TestCase):

    @mock.patch.object(wsgi.Loader, 'load_app', mock.Mock())
    def test_launch_app(self):
        self.service = service.WSGIService("test_service")
        self.assertEqual(0, self.service.port)
        launcher = service.Launcher()
        launcher.launch_server(self.service)
        self.assertEqual(0, self.service.port)
        launcher.stop()
        wsgi.Loader.load_app.assert_called_once_with("test_service")
