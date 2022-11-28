import pstats
from pstats import SortKey

# add following to your interpreter options in PyCharm run configuration : -m cProfile -o profile-data.txt
# you can supply above as command line parameter to the python interpreter if you run from command line

p = pstats.Stats('profile-data')
# print all stats
# p.strip_dirs().sort_stats(-1).print_stats()
# removed dir path form entries ; primary sort is CUMULATIVE ; print first 15 entries
p.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats(15)
# primary sort  key is TIME ; print first 15 entries
p.sort_stats(SortKey.TIME).print_stats(15)
# primary sort key is TIME, secondary is CUMULATIVE ; print 10% of all lines that match __init__
p.sort_stats(SortKey.TIME, SortKey.CUMULATIVE).print_stats(.1, '__init__')
