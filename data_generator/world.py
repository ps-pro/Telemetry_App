import networkx as nx
import random
from typing import List, Tuple, Optional

# For clarity, let's define a type alias for a node.
NodeType = Tuple[int, int]

class GridWorld:
    """
    Represents the simulated grid-based world for vehicle telemetry.

    This class encapsulates the road network as a graph, where intersections
    are nodes and roads are edges. It provides core functionalities like
    route generation and finding points of interest (e.g., refueling stations).

    Attributes:
        graph (nx.Graph): The underlying graph representation of the world.
        width (int): The number of vertical roads in the grid.
        height (int): The number of horizontal roads in the grid.
        refueling_stations (List[NodeType]): A list of nodes designated as
                                             refueling stations.
    """

    def __init__(self, width: int, height: int, num_refueling_stations: int = 10, verbose: bool = True):
        """
        Initializes the GridWorld with a specified size.

        Args:
            width (int): The number of vertical roads (nodes will be from 0 to width).
            height (int): The number of horizontal roads (nodes will be from 0 to height).
            num_refueling_stations (int): The number of refueling stations to
                                          randomly place in the world.

        Raises:
            ValueError: If width, height, or num_refueling_stations are not positive.
        """
        if not (width > 0 and height > 0):
            raise ValueError("Grid width and height must be positive integers.")
        if num_refueling_stations < 0:
            raise ValueError("Number of refueling stations cannot be negative.")

        self.width = width
        self.height = height
        self.graph = self._create_grid_graph()
        
        # Ensure we don't try to create more stations than there are nodes
        if num_refueling_stations > len(self.graph.nodes):
            raise ValueError("Cannot have more refueling stations than nodes in the graph.")
            
        self.refueling_stations = self._place_refueling_stations(num_refueling_stations)


        if verbose:
            print(f"GridWorld created: {self.width}x{self.height} grid with {nx.number_of_nodes(self.graph)} nodes.")
            print(f"Placed {len(self.refueling_stations)} refueling stations.")


        print(f"GridWorld created: {self.width}x{self.height} grid with {nx.number_of_nodes(self.graph)} nodes.")
        print(f"Placed {len(self.refueling_stations)} refueling stations.")

    def _create_grid_graph(self) -> nx.Graph:
        """
        Generates a 2D grid graph using networkx.

        Nodes are represented as (x, y) tuples. Edges are created between
        adjacent nodes, representing roads. Each edge is given a 'weight'
        attribute of 1, representing a 1km distance.
        """
        G = nx.grid_2d_graph(self.width, self.height)
        # We can add attributes to edges if needed later, e.g., speed limits
        nx.set_edge_attributes(G, 1, "weight")
        return G

    def _place_refueling_stations(self, num_stations: int) -> List[NodeType]:
        """
        Randomly selects nodes to be refueling stations.
        """
        nodes = list(self.graph.nodes())
        return random.sample(nodes, num_stations)

    def get_random_route(self, min_distance: int = 5) -> Optional[List[NodeType]]:
        """
        Generates a random, valid route between two distinct nodes.

        The route is a list of nodes (intersections) to be traversed. It uses
        the shortest path algorithm (Dijkstra's, which is equivalent to BFS
        on an unweighted graph) to ensure a logical path.

        Args:
            min_distance (int): The minimum required length of the route in nodes.
                                This prevents trivial routes from being generated.

        Returns:
            An optional list of nodes representing the path, or None if a valid
            route cannot be found.
        """
        nodes = list(self.graph.nodes())
        
        # Ensure we can find a route that meets the minimum distance
        for _ in range(10): # Try up to 10 times to find a suitable route
            start_node, end_node = random.sample(nodes, 2)
            
            # Use networkx's highly optimized shortest path algorithm
            route = nx.shortest_path(self.graph, source=start_node, target=end_node, weight="weight")
            
            if len(route) >= min_distance:
                return route
        
        # If we failed to find a suitable route after several tries, return None
        print(f"Warning: Could not find a random route with min distance {min_distance} after 10 attempts.")
        return None

    def find_nearest_refueling_station(self, start_node: NodeType) -> Tuple[NodeType, List[NodeType]]:
        """
        Finds the closest refueling station to a given node.

        Args:
            start_node (NodeType): The node from which to start the search.

        Returns:
            A tuple containing:
            - The destination node of the closest refueling station.
            - The list of nodes representing the path to that station.
            
        Raises:
            ValueError: If there are no refueling stations in the world.
        """
        if not self.refueling_stations:
            raise ValueError("No refueling stations exist in this world.")

        # Calculate shortest path lengths from the start_node to all other nodes
        path_lengths = nx.single_source_shortest_path_length(self.graph, start_node)

        # Find the refueling station with the minimum path length
        closest_station = min(
            self.refueling_stations,
            key=lambda station: path_lengths.get(station, float('inf'))
        )
        
        # Now that we know the closest, calculate the actual path to it
        route_to_station = nx.shortest_path(self.graph, source=start_node, target=closest_station, weight="weight")
        
        return closest_station, route_to_station


# The example usage section at the bottom:
if __name__ == "__main__":
    # 1. Create a 100km x 100km world
    print("Initializing GridWorld...")
    world = GridWorld(width=101, height=101, num_refueling_stations=100)

    # 2. Generate a random trip for a vehicle
    print("\nGenerating a random route...")
    random_route = world.get_random_route(min_distance=15)
    if random_route:
        print(f"  - Generated a route with {len(random_route)} nodes (approx {len(random_route)-1} km).")
        print(f"  - Start: {random_route[0]}, End: {random_route[-1]}")

    # 3. Find the nearest gas station from a random point
    print("\nFinding the nearest refueling station...")
    start_point = random.choice(list(world.graph.nodes()))
    print(f"  - Starting search from node: {start_point}")
    closest_station_node, route_to_station = world.find_nearest_refueling_station(start_point)
    print(f"  - Closest station is at {closest_station_node}.")
    print(f"  - The route to it is {len(route_to_station)} nodes long.")