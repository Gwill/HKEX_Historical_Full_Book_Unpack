# coding: utf-8

import struct


class Unpacker(object):

    def __init__(self, packed_data_file_path):

        # 载入数据
        with open(packed_data_file_path, 'rb') as f:
            self.data = f.read()

        self.data_len = len(self.data)
        
        self.offset = 0
        self.result = []


    def _unpack_RecLen(self):
        field_len = 2

        d = self.data[self.offset:self.offset+2]
        self.offset += 2

        assert len(d) == field_len
        RecLen = struct.unpack('H', d)[0]

        return RecLen

    def _unpack_RecLen


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


    def test(self):
        # offset = 6144
        # print(self._unpack_RecLen(self.data[offset:offset+2]))
        print(self._rec_count())


if __name__ == '__main__':
    data_file = 'MC01_All_20130904'

    unpacker = Unpacker(data_file)
    # unpacker.test()
