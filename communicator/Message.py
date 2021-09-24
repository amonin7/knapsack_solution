import sequential.main as ma


class Message:

    def __init__(self, message_type, payload=None):
        self.message_type = message_type
        self.payload = payload


def pack(m: Message):
    if m.message_type == "get_request":
        # payload is just number. ex. payload = 7
        return {
            "message_type": m.message_type,
            "payload": m.payload
        }
    elif m.message_type == "subproblems":
        # payload is a list of Nodes. ex. payload = [Node(0,0,0,0), Node(1,0,0,0), Node(2,0,0,0)]
        payload = list()
        for node in m.payload:
            payload.append(ma.nodeToDict(node))
        return {
            "message_type": m.message_type,
            "payload": payload
        }
    elif m.message_type == "exit_command":
        # payload is just nothing. ex. payload = None
        return {
            "message_type": m.message_type,
            "payload": m.payload
        }
    elif m.message_type == "T":
        # payload is just number ex. payload = -1
        return {
            "message_type": m.message_type,
            "payload": m.payload
        }


def unpack(d: dict) -> Message:
    if d["message_type"] == "get_request":
        return Message(d["message_type"], d["payload"])
    elif d["message_type"] == "subproblems":
        payload = list()
        for node_dict in d["payload"]:
            payload.append(ma.dictToNode(node_dict))
        return Message(d["message_type"], payload)
    elif d["message_type"] == "exit_command":
        return Message(d["message_type"])
    elif d["message_type"] == "T":
        return Message(d["message_type"], d["payload"])
