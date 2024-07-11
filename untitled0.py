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
W.addNode('Start0', 1, 0)
W.addNode('Start1', 2, 0)
W.addLink('Link0', 'Start0', 'Start1', length=500, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

#Part2
W.addNode('LStreet1', 3, 2)
W.addNode('LStreet2', 3, 1)
W.addNode('LStreet3', 3, 0)
W.addLink('LGroup1', 'Start1', 'LStreet1', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('LGroup2', 'Start1', 'LStreet2', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('LGroup3', 'Start1', 'LStreet3', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

#Part3
W.addNode('RStreet1', 6, 2)
W.addNode('RStreet2', 6, 1)
W.addNode('RStreet3', 6, 0)
W.addLink('Street1', 'LStreet1','RStreet1', length=1000, free_flow_speed=10, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('Street2', 'LStreet2','RStreet2', length=1000, free_flow_speed=10, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('Street3', 'LStreet3','RStreet3', length=1000, free_flow_speed=10, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

#Part4
W.addNode('Dest1', 7,0)
W.addLink('RGroup1', 'RStreet1','Dest1', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('RGroup2', 'RStreet2','Dest1', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)
W.addLink('RGroup3', 'RStreet3','Dest1', length=1000, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)

#Part5
W.addNode('Dest0', 8, 0)
W.addLink('Link1', 'Dest1', 'Dest0', length=500, free_flow_speed=20, number_of_lanes = 1, jam_density=0.2, merge_priority=0.5)



#Demands
W.adddemand('Start0', 'Dest0', 0, 3600, 0.4)
W.adddemand('Start0', 'Dest0', 200, 3600, 0.4)
W.adddemand('Start0', 'Dest0', 500, 3600, 0.4)
#W.adddemand('Start1', 'RStreet1', 100, 3000, 0.2)

#Execute
W.exec_simulation()

W.analyzer.print_simple_stats()

W.show_network()

# #Create GIF
# W.analyzer.network_fancy(animation_speed_inverse=15, sample_ratio=0.3, interval=3, trace_length=5, network_font_size=1)

# with open("out/anim_network_fancy.gif", "rb") as f:
#     display(Image(data=f.read(), format='png'))
    
#Create GIF    
W.analyzer.network_anim(detailed=0, network_font_size=1, figsize=(6,6))

from IPython.display import display, Image
with open("/Users/twokdin/Documents/GitHub/Big-Data/out/anim_network0.gif", "rb") as f:
    display(Image(data=f.read(), format='png'))