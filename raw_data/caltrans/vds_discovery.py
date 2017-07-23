import sys
import math # To calculate distance
import xml.etree.ElementTree as ET  # For reading XML file

def main(argv):
    # Get latitude, longitude, and radius float values from user
    try:
        loc = [float(input("Latitude of location: "))]
        loc.append(float(input("Longitude of location: ")))
        radius = float(input("Radius extending from location: "))
    except:
        print("Please enter a numerical value\n")
    
    # Parse XML file in order to get information for VDSs.
    tree = ET.parse("vds_config.xml")
    root = tree.getroot()
    stations = root[11][1]  # Gets detector_stations child tag in XML file for District 11

    # Loops thru all of the VDSs to find if they are within the radius
    first_id = True  # For IDs.txt
    first_otherid = True  # For otherIDs.txt
    for vds in stations.findall("vds"):
        # Get latitude, longitude, and ID for VDS
        vds_id = vds.get("id")
        try:
            vds_loc = [float(vds.get("latitude"))]
            vds_loc.append(float(vds.get("longitude")))
        except:
            print("Could not get latitude and longitude for {}".format(vds_id))
            pdb.set_trace()

        # Compare distance from VDS to location to radius. If within boundaries, writes to file.
        if distance(loc, vds_loc) <= radius:
            # Writes VDS ID to file IDs.txt if the VDS is a mainline or HOV (these have more data).
            if vds.get("type") == "ML" or vds.get("type") == "HV":
                write_file(first_id, "IDs.txt", vds_id)
                first_id = False  # Sets first_id to false, so that it will only append afterwards.
            # Otherwise writes to otherIDs.txt. Follows same writing format as for IDs.txt.
            else:  # Same procedure as if mainline or HOV, but stored in different file
                write_file(first_otherid, "otherIDs.txt", vds_id)
                first_otherid = False


def distance(loc1, loc2):
    """Calculate distance between the entered coordinates and the coordinates of the VDS."""
    return math.sqrt((loc2[0]-loc1[0])**2 + (loc2[1]-loc1[1])**2)  # sqrt((x2-x1)^2+(y2-y1)^2)


def write_file(first_check, filename, identifier):
    """Writes ID to appropriate file."""
    if first_check == True:  # If first ID to be written (overwrite file as precaution)
        with open(filename, "w") as output_file:
            output_file.write("{}\n".format(identifier))
    else:
        with open(filename, "a") as output_file:  # All other IDs should be appended
            output_file.write("{}\n".format(identifier))


# Executes from here
if __name__ == "__main__":
    main(sys.argv[1:])
