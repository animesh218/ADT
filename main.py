from utils.plasdb import main2
from utils.fixed_prop import main1
from utils.beauty import main4
from utils.category import main5
import os
import sys

input_file = r"utils/hp_targeting.csv"
output_file = r"output/hp_output1.csv"
os.system(f'python utils/target.py {input_file} {output_file}')
# Note the space between target.py and {input_file}/

# Execute main1
main1()
# Execute main2
main2()
# Execute main
main4()

main5()