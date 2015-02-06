#!/usr/bin/python
import sys

def main(argv):
  buffer_len = 0
  buffer_arr = ""
  line = sys.stdin.readline()
  try:
    while line:
      line = line.rstrip()
      buffer_len = buffer_len + len(line)
      if buffer_len > 19:
        print "value1:" + str(buffer_len) + "," + buffer_arr
        buffer_arr = line
        buffer_len = len(line)
      else:
        buffer_arr = buffer_arr+line+"\n"
      line = sys.stdin.readline()
  except "end of file":
    print "value2:" + str(buffer_len) + "," + buffer_arr
    return None
if __name__ == "__main__":
  main(sys.argv)