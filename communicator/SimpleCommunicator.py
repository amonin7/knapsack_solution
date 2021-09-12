import communicator.Message as me
from mpi4py import MPI


class SimpleCommunicator:

    def send(self, receiver, message: me.Message, comm):
        # print(f"receiver: {receiver}")
        comm.send(me.pack(message), dest=receiver)
        return "sent_" + message.message_type, []

    def receive(self, comm):
        status = MPI.Status()
        message_dict = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        message = me.unpack(message_dict)
        return "received_" + message.message_type, message
