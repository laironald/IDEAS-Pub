import sys
import IDEAS.api

proj = "pop"

def main(args):
  if not args:
    print "ERROR: You must specify an option"
    print "python ingest.py [option]"
    print ""
    print "[options]:"
    print "  backup    generate a tar with MySQL data"
    print "  drop      ingest all csv files on the current directory"
    print "  ingest    clear the cache"
  elif args[0] == "drop":
    IDEAS.api.remove_tables(proj)
  elif args[0] == "ingest":
    IDEAS.api.ingest_all(override=True)
  elif args[0] == "backup":
    IDEAS.api.backup_cmd(proj)

if __name__ == "__main__":
  main(sys.argv[1:])
