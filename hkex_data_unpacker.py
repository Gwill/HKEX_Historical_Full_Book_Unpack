# coding: utf-8

import struct
from datetime import datetime


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

    def _parse_msg_data(self, MsgType, MsgData):
        return

    def _data_hex(self, data):
        return ''.join(['{:02X}'.format(ord(d)) for d in data])


    def _parse_MarketDefinition(self, data, result):

        result['MarketCode'] = struct.unpack('4s', data[0:4])[0].strip()
        result['MarketName'] = struct.unpack('25s', data[4:4+25])[0].strip()
        result['CurrencyCode'] = struct.unpack('3s', data[29:29+3])[0].strip()
        result['NumberOfSecurities'] = struct.unpack('I', data[32:32+4])[0]

        return result

    def _parse_SecurityDefinition(self, data, result):
        data_len = len(data)

        spec = [
            ('SecurityCode', 'I', 4),
            ('MarketCode', '4s', 4),
            ('ISINCode', '12s', 12),
            ('InstrumentType', '4s', 4),
            ('SpreadTableCode', '2s', 2),
            ('SecurityShortName', '40s', 40),
            ('CurrencyCode', '3s', 3),
            ('SecurityNameGCCS', '60s', 60),
            ('SecurityNameGB', '60s', 60),
            ('LotSize', 'I', 4),
            ('PreviousClosingPrice', 'i', 4),
            ('Filler', '1s', 1),
            ('ShortSellFlag', '1s', 1),
            ('Filler', '1s', 1),
            ('CCASSFlag', '1s', 1),
            ('DummySecurityFlag', '1s', 1),
            ('TestSecurityFlag', '1s', 1),
            ('StampDutyFlag', '1s', 1),
            ('Filler', '1s', 1),
            ('ListingDate', 'I', 4),
            ('DelistingDate', 'I', 4),
            ('FreeText', '38s', 38),
            ('EFNFlag', '1s', 1),
            ('AccruedInterest', 'I', 4),
            ('CouponRate', 'I', 4),
            ('ConversionRatio', 'I', 4),
            ('StrikePrice', 'i', 4),
            ('MaturityDate', 'I', 4),
            ('CallPutFlag', '1s', 1),
            ('Style', '1s', 1),
            ('NoUnderlyingSecurities', 'H', 2),
            # ('UnderlyingSecurityCode', 'I', 4),
            # ('UnderlyingSecurityWeight', 'I', 4),
        ]

        offset = 0

        for s in spec:
            result[s[0]] = struct.unpack(s[1], data[offset:offset+s[2]])[0]

            if s[0] == 'SecurityNameGCCS':
                result['SecurityNameGCCS'] = result['SecurityNameGCCS'].decode('utf-16').encode('utf-8')
                # print('SecurityNameGCCS: {}'.format(result['SecurityNameGCCS']))

            if s[0] == 'SecurityNameGB':
                result['SecurityNameGB'] = result['SecurityNameGB'].decode('utf-16').encode('utf-8')
                # print('SecurityNameGB: {}'.format(result['SecurityNameGB']))

            if isinstance(result[s[0]], str):
                result[s[0]] = result[s[0]].strip()

            offset += s[2]

        return result

    def _parse_LiquidityProvider(self, data, result):
        pass

    def _parse_CurrentRate(self, data, result):
        pass


    def unpack(self):

        TotalMsg = 0

        while True:

            RecLen = self._unpack_RecordLength_RecLen()

            # print('RecLen: {}'.format(RecLen))

            PktSize = self._unpack_PacketHeader_PktSize()
            MsgCount = self._unpack_PacketHeader_MsgCount()
            Filler = self._unpack_PacketHeader_Filler()
            SeqNum = self._unpack_PacketHeader_SeqNum()
            SendTime = self._unpack_PacketHeader_SendTime()

            # print('PktSize: {}, MsgCount: {}, Filler: {}, SeqNum: {}, SendTime: {}'.format(
            #     PktSize, MsgCount, self._data_hex(Filler), SeqNum, SendTime
            # ))

            while MsgCount:

                MsgSize = self._unpack_Msg_MsgSize()
                MsgType = self._unpack_Msg_MsgType()
                MsgData = self._unpack_Msg_MsgData()


                MsgCount -= 1

                TotalMsg += 1


                result = {}
                result['MsgType'] = MsgType
                result['SendTime'] = datetime.fromtimestamp(SendTime/1000000000.0).strftime('%Y-%m-%d %H:%M:%S.%f')

                # Market Definition
                if MsgType == 10:
                    result = self._parse_MarketDefinition(MsgData, result)
                    self.MarketDefinition_results.append(result)

                # Security Definition
                if MsgType == 11:
                    result = self._parse_SecurityDefinition(MsgData, result)
                    self.SecurityDefinition_results.append(result)
                    print(result)
                    print('MsgSize: {}, MsgType: {}, MsgData: {}'.format(MsgSize, MsgType, self._data_hex(MsgData)))


                # if MsgType != 100 and MsgType != 11:

                #     


            if not self._check_valid_offset():
                break

        print('TotalMsg: {}'.format(TotalMsg))
        print('Total MarketDefinition Results: {}'.format(len(self.MarketDefinition_results)))


if __name__ == '__main__':
    data_file = 'MC01_All_20130904'

    unpacker = Unpacker(data_file)
    unpacker.unpack()
