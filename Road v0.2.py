from uxsim import *
import itertools

seed = None

W = World(
    name="",
    deltan=5,
    tmax=3600, #1 hour simulation
    print_mode=1, save_mode=0, show_mode=1,
    random_seed=seed,
    duo_update_time=600
)
random.seed(seed)

#Scenario


#Part1 
W.addNode('Start01', 1, 0)
W.addNode('Start11', 2, 0)
W.addLink('Link01', 'Start01', 'Start11', length=500, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

W.addNode('Start02', 1, 1)
W.addNode('Start12', 2, 1)
W.addLink('Link02', 'Start02', 'Start12', length=500, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

W.addNode('Start03', 1, 2)
W.addNode('Start13', 2, 2)
W.addLink('Link03', 'Start03', 'Start13', length=500, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)


#Part2
W.addNode('LStreet1', 3, 2)
W.addNode('LStreet2', 3, 1)
W.addNode('LStreet3', 3, 0)
W.addLink('LGroup11', 'Start11', 'LStreet1', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('LGroup12', 'Start11', 'LStreet2', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('LGroup13', 'Start11', 'LStreet3', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

W.addLink('LGroup21', 'Start12', 'LStreet1', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('LGroup22', 'Start12', 'LStreet2', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('LGroup23', 'Start12', 'LStreet3', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

W.addLink('LGroup31', 'Start13', 'LStreet1', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('LGroup32', 'Start13', 'LStreet2', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('LGroup33', 'Start13', 'LStreet3', length=1000, free_flow_speed=50, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

#Part3
#W.addNode('RStreet1', 6, 2)
#W.addNode('RStreet2', 6, 1)
#W.addNode('RStreet3', 6, 0)
# W.addLink('Street1', 'LStreet1','Dest1', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
# W.addLink('Street2', 'LStreet2','Dest1', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
# W.addLink('Street3', 'LStreet3','Dest1', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

#Part4
W.addNode('Dest1', 7,1)
W.addLink('RGroup1', 'LStreet1','Dest1', length=1000, free_flow_speed=30, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('RGroup2', 'LStreet2','Dest1', length=1000, free_flow_speed=30, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('RGroup3', 'LStreet3','Dest1', length=1000, free_flow_speed=30, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

#Part5
W.addNode('Dest0', 8, 1)
W.addLink('Link1', 'Dest1', 'Dest0', length=500, free_flow_speed=50, number_of_lanes = 2)



#Demands
W.adddemand('Start01', 'Dest0', 0, 3600, 0.4)
W.adddemand('Start01', 'Dest0', 200, 600, 0.8)
W.adddemand('Start02', 'Dest0', 500, 3000, 0.4)
W.adddemand('Start02', 'Dest0', 100, 1000, 0.8)
W.adddemand('Start03', 'Dest0', 100, 2000, 0.4)
W.adddemand('Start03', 'Dest0', 400, 3000, 0.8)

#Execute
W.exec_simulation()

W.analyzer.print_simple_stats()

W.analyzer.vehicle_trip_to_pandas()

W.show_network()

# #Create GIF
W.analyzer.network_fancy(animation_speed_inverse=15, sample_ratio=0.3, interval=3, trace_length=5, network_font_size=1)

with open("out/anim_network_fancy.gif", "rb") as f:
    display(Image(data=f.read(), format='png'))
    
#Create GIF    
W.analyzer.network_anim(detailed=0, network_font_size=1, figsize=(6,6))

from IPython.display import display, Image
with open("out/anim_network0.gif", "rb") as f:
    display(Image(data=f.read(), format='png'))