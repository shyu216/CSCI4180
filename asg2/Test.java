public class Test {
          byte firstByte = System.in.readByte();
    int len = decodeVIntSize(firstByte);
        if (len == 1) {
          return firstByte;
        }
      long i = 0;
        for (int idx = 0; idx < len-1; idx++) {
          byte b = 0x11;
          i = i << 8;
          i = i | (b & 0xFF);
        }
        return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
}