# coding: utf-8

import struct


class Unpacker(object):

    def __init__(self, packed_data_file_path):

        # 载入数据
        with open(packed_data_file_path, 'rb') as f:
            self.data = f.read()

        self.data_len = len(self.data)
        
        self.offset = 0

        self.current_msg_len = 0

        self.MarketDefinition_results = []
        self.SecurityDefinition_results = []
        self.LiquidityProvider_results = []
        self.CurrentRate_results = []


    def _unpack_RecordLength_RecLen(self):
        field_len = 2

        d = self.data[self.offset:self.offset+field_len]
        self.offset += field_len

        assert len(d) == field_len
        RecLen = struct.unpack('H', d)[0]

        return RecLen

    def _unpack_PacketHeader_PktSize(self):
        field_len = 2

        d = self.data[self.offset:self.offset+field_len]
        self.offset += field_len
        assert len(d) == field_len

        PktSize = struct.unpack('H', d)[0]

        return PktSize

    def _unpack_PacketHeader_MsgCount(self):
        field_len = 1

        d = self.data[self.offset:self.offset+field_len]
        self.offset += field_len

        # @todo:
        MsgCount = struct.unpack('B', d)[0]
        # MsgCount = 1

        return MsgCount

    def _unpack_PacketHeader_Filler(self):
        field_len = 1

        field_data = self.data[self.offset:self.offset+field_len]
        self.offset += field_len

        Filler = struct.unpack('s', field_data)[0]

        return Filler

    def _unpack_PacketHeader_SeqNum(self):
        field_len = 4

        field_data = self.data[self.offset:self.offset+field_len]
        self.offset += field_len

        # @todo: int
        SeqNum = struct.unpack('I', field_data)[0]

        return SeqNum

    def _unpack_PacketHeader_SendTime(self):
        field_len = 8

        field_data = self.data[self.offset:self.offset+field_len]
        self.offset += field_len

        # @todo: long
        SendTime = struct.unpack('Q', field_data)[0]

        return SendTime

    def _unpack_Msg_MsgSize(self):
        field_len = 2

        field_data = self.data[self.offset:self.offset+field_len]
        self.offset += field_len

        # @todo:
        MsgSize = struct.unpack('H', field_data)[0]

        # @memo:
        self.current_msg_len = MsgSize - 4

        return MsgSize

    def _unpack_Msg_MsgType(self):
        field_len = 2

        field_data = self.data[self.offset:self.offset+field_len]
        self.offset += field_len

        # @todo:
        MsgType = struct.unpack('H', field_data)[0]
        return MsgType

    def _unpack_Msg_MsgData(self):

        assert self.current_msg_len != 0

        MsgData = self.data[self.offset:self.offset+self.current_msg_len]

        self.offset += self.current_msg_len

        return MsgData


    def _check_valid_offset(self):
        if self.offset == self.data_len:
            return False
        return True

    # def _rec_count(self):
    #     count = 0
    #     offset = 0

    #     while self.data_len > 0:
    #         current_RecLen = self._unpack_RecLen(self.data[offset:offset+2])

    #         print('current_RecLen', current_RecLen)

    #         self.data_len = self.data_len - current_RecLen

    #         print('self.data_len', self.data_len)

    #         offset = offset + current_RecLen

    #         print('offset', offset)

    #         count = count + 1
    #         print('count', count)
    #         #break

    #     return count

    def _parse_msg_data(self, MsgType, MsgData):
        return

    def _data_hex(self, data):
        return ''.join(['{:02X}'.format(ord(d)) for d in data])



    def test(self):
        # offset = 6144
        # print(self._unpack_RecLen(self.data[offset:offset+2]))
        # print(self._rec_count())

        counter = 30

        while counter:

            RecLen = self._unpack_RecordLength_RecLen()

            print('RecLen: {}'.format(RecLen))

            PktSize = self._unpack_PacketHeader_PktSize()
            MsgCount = self._unpack_PacketHeader_MsgCount()
            Filler = self._unpack_PacketHeader_Filler()
            SeqNum = self._unpack_PacketHeader_SeqNum()
            SendTime = self._unpack_PacketHeader_SendTime()

            print('PktSize: {}, MsgCount: {}, Filler: {}, SeqNum: {}, SendTime: {}'.format(
                PktSize, MsgCount, self._data_hex(Filler), SeqNum, SendTime
            ))

            while MsgCount:

                MsgSize = self._unpack_Msg_MsgSize()
                MsgType = self._unpack_Msg_MsgType()
                MsgData = self._unpack_Msg_MsgData()


                MsgCount -= 1

                if MsgType != 100 and MsgType != 11:

                    print('MsgSize: {}, MsgType: {}, MsgData: {}'.format(MsgSize, MsgType, self._data_hex(MsgData)))


            if not self._check_valid_offset():
                break


            # counter -= 1
            # pass



if __name__ == '__main__':
    data_file = 'MC01_All_20130904'

    unpacker = Unpacker(data_file)
    unpacker.test()
