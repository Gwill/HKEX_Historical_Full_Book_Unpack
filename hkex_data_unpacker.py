# coding: utf-8

import struct


class Unpacker(object):

    def __init__(self, packed_data_file_path):

        with open(packed_data_file_path, 'rb') as f:
            self.data = f.read()

        self.data_len = len(self.data)

        print('self.data_len', self.data_len)

        self.offset = 0


    def _unpack_RecLen(self, d):
        assert len(d) == 2
        RecLen = struct.unpack('H', d)[0]
        return RecLen


    def _rec_count(self):
        count = 0
        offset = 0

        while self.data_len > 0:
            current_RecLen = self._unpack_RecLen(self.data[offset:offset+2])

            print('current_RecLen', current_RecLen)

            self.data_len = self.data_len - current_RecLen

            print('self.data_len', self.data_len)

            offset = offset + current_RecLen

            print('offset', offset)

            count = count + 1
            print('count', count)
            #break

        return count


    def test(self):
        # offset = 6144
        # print(self._unpack_RecLen(self.data[offset:offset+2]))
        print(self._rec_count())


if __name__ == '__main__':
    data_file = 'MC01_All_20130904'

    unpacker = Unpacker(data_file)
    # unpacker.test()
