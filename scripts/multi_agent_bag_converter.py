# Copyright (C) 2024 Yanxin Zhou
# 
# This file is part of ws_mutli_slam.
# 
# ws_mutli_slam is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# ws_mutli_slam is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with ws_mutli_slam.  If not, see <http://www.gnu.org/licenses/>.

import rosbag
import rospy

def align_bag_times(original_bag_path, output_bag_path, remap_rules=None):
    """
    Splits a ROS bag into three parts, remaps topics (if rules provided), and aligns the start times of all parts.

    :param original_bag_path: Path to the original ROS bag.
    :param output_bag_path: Path for the output merged bag.
    :param remap_rules: Dictionary with old topic names as keys and new names as values.
    """
    segment_paths = ['part_1.bag', 'part_2.bag', 'part_3.bag']
    start_times = []
    end_times = []

    # Step 1: Split the bag into three parts and record start and end times
    with rosbag.Bag(original_bag_path, 'r') as original_bag:
        start_time = original_bag.get_start_time()
        end_time = original_bag.get_end_time()
        duration = end_time - start_time
        segment_duration = duration / 3

        for i in range(3):
            segment_start = start_time + i * segment_duration
            segment_end = start_time + (i + 1) * segment_duration if i < 2 else end_time
            start_times.append(segment_start)
            end_times.append(segment_end)

            with rosbag.Bag(segment_paths[i], 'w') as segment_bag:
                for topic, msg, t in original_bag.read_messages(start_time=rospy.Time.from_sec(segment_start),
                                                               end_time=rospy.Time.from_sec(segment_end)):
                    if remap_rules and topic in remap_rules:
                        topic = "/jackal" + str(i) + remap_rules[topic]
                    segment_bag.write(topic, msg, t)

    # Step 2: Align start times and merge the bags
    with rosbag.Bag(output_bag_path, 'w') as output_bag:
        for i, segment_path in enumerate(segment_paths):
            time_shift = rospy.Duration.from_sec(start_times[0] - start_times[i])
            with rosbag.Bag(segment_path, 'r') as segment_bag:
                for topic, msg, t in segment_bag.read_messages():
                    new_t = t + time_shift
                    output_bag.write(topic, msg, new_t)

    print(f"Created merged bag with aligned start times: {output_bag_path}")


# Example Usage
original_bag_path = "./2023-06-28-10-00-30.bag"  # Replace with your bag file path
output_bag_path = "merged_output.bag"
remap_rules = {'/livox/lidar': '/livox/lidar', '/livox/imu': '/livox/imu'}  # Replace with your topics

align_bag_times(original_bag_path, output_bag_path, remap_rules)
