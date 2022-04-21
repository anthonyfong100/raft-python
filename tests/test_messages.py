import unittest
import raft_python.messages as messages


class TestMessageTypes(unittest.TestCase):
    def test_serialization_hello(self):
        hello_req = messages.HelloMessage(
            "src", "dst", "leader")
        expected_payload: dict = {
            "src": "src",
            "dst": "dst",
            "type": "hello",
            "leader": "leader",
        }
        self.assertDictEqual(hello_req.serialize(), expected_payload,
                             "Hello request serialization should be identical")

    def test_serialization_get(self):
        get_req = messages.GetMessageRequest(
            "src", "dst", "MID", "key", "leader")
        expected_payload: dict = {
            "src": "src",
            "dst": "dst",
            "MID": "MID",
            "type": "get",
            "leader": "leader",
            "key": "key"
        }
        self.assertDictEqual(get_req.serialize(), expected_payload,
                             "Get request serialization should be identical")

    def test_serialization_get_ok(self):
        get_req_ok = messages.GetMessageResponseOk(
            "src", "dst", "MID", "value", "leader")
        expected_payload: dict = {
            "src": "src",
            "dst": "dst",
            "MID": "MID",
            "type": "ok",
            "leader": "leader",
            "value": "value"
        }
        self.assertDictEqual(get_req_ok.serialize(), expected_payload,
                             "Get ok response serialization should be identical")

    def test_serialization_put(self):
        put_req = messages.PutMessageRequest(
            "src", "dst", "MID", "key", "value", "leader")
        expected_payload: dict = {
            "src": "src",
            "dst": "dst",
            "MID": "MID",
            "type": "put",
            "leader": "leader",
            "key": "key",
            "value": "value",
        }
        self.assertDictEqual(put_req.serialize(), expected_payload,
                             "Put response serialization should be identical")

    def test_serialization_put_ok(self):
        put_req_ok = messages.PutMessageResponseOk(
            "src", "dst", "MID", "leader")
        expected_payload: dict = {
            "src": "src",
            "dst": "dst",
            "MID": "MID",
            "type": "ok",
            "leader": "leader",
        }
        self.assertDictEqual(put_req_ok.serialize(), expected_payload,
                             "Put ok response serialization should be identical")

    def test_serialization_fail(self):
        req_fail = messages.MessageFail(
            "src", "dst", "MID", "leader")
        expected_payload: dict = {
            "src": "src",
            "dst": "dst",
            "MID": "MID",
            "type": "fail",
            "leader": "leader",
        }
        self.assertDictEqual(req_fail.serialize(), expected_payload,
                             "Fail response serialization should be identical")

    def test_serialization_redirect(self):
        req_redirect = messages.MessageRedirect(
            "src", "dst", "MID", "leader")
        expected_payload: dict = {
            "src": "src",
            "dst": "dst",
            "MID": "MID",
            "type": "redirect",
            "leader": "leader",
        }
        self.assertDictEqual(req_redirect.serialize(), expected_payload,
                             "redirect response serialization should be identical")


if __name__ == '__main__':
    unittest.main()
