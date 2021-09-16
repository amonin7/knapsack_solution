import communicator.Message as me
from mpi4py import MPI


class SimpleCommunicator:

    def __init__(self, comm):
        self.comm = comm

    def send(self, receiver, message: me.Message):
        self.comm.send(me.pack(message), dest=receiver)
        return "sent_" + message.message_type, []

    def receive(self):
        status = MPI.Status()
        message_dict = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        message = me.unpack(message_dict)
        sender = status.Get_source()
        return "received_" + message.message_type, message, sender
