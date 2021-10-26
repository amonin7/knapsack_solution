import sequential.Solver as ma


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
        # payload is a list of Nodes.
        # ex. payload = {'problems': [Node(0,0,0,0), Node(1,0,0,0), Node(2,0,0,0)], 'record': 345}
        problems = list()
        for node in m.payload['problems']:
            problems.append(ma.node_to_dict(node))
        return {
            "message_type": m.message_type,
            "payload": {
                'problems': problems,
                'record': m.payload['record']
            }
        }
    elif m.message_type == "exit_command":
        # payload is just nothing. ex. payload = None
        return {
            "message_type": m.message_type,
            "payload": m.payload
        }
    elif m.message_type == "S":
        # payload is just number ex. payload = {'S': 0, 'record': 345}
        return {
            "message_type": m.message_type,
            "payload": m.payload
        }


def unpack(d: dict) -> Message:
    if d["message_type"] == "get_request":
        return Message(d["message_type"], d["payload"])
    elif d["message_type"] == "subproblems":
        problems = list()
        for node_dict in d["payload"]['problems']:
            problems.append(ma.dict_to_node(node_dict))
        return Message(d["message_type"], {'problems': problems, 'record': d["payload"]['record']})
    elif d["message_type"] == "exit_command":
        return Message(d["message_type"])
    elif d["message_type"] == "S":
        return Message(d["message_type"], d["payload"])
