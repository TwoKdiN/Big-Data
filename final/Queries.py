from pymongo import MongoClient

# Σύνδεση με τη MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.vehicles_data
processed_collection = db.processedData
raw_collection = db.rawData

# Ορισμός χρονικής περιόδου
start_time = "18/12/2024 16:12:24"
end_time = "18/12/2024 16:15:24"

print("---------------------1------------------------")

# Query 1: Ακμές με το μικρότερο πλήθος οχημάτων (vcount)
min_vcount = processed_collection.find_one({
    "time": {"$gte": start_time, "$lte": end_time}
}, sort=[("vcount", 1)])['vcount']

smallest_vcount_edge = processed_collection.find({
    "time": {"$gte": start_time, "$lte": end_time},
    "vcount": min_vcount
})

# Εκτύπωση αποτελέσματος για το μικρότερο πλήθος οχημάτων
print("Ακμές με το μικρότερο πλήθος οχημάτων:")
seen_links = set()  # Σύνολο για την καταγραφή των εμφανισμένων ακμών
for edge in smallest_vcount_edge:
    link = edge['link']
    if link not in seen_links:
        seen_links.add(link)
        print(f"Ακμή: {link}, Πλήθος οχημάτων: {edge['vcount']}")

print("---------------------2------------------------")

# Query 2: Βρες τη μέγιστη μέση ταχύτητα
max_vspeed = processed_collection.find_one({
    "time": {"$gte": start_time, "$lte": end_time}
}, sort=[("vspeed", -1)])['vspeed']

largest_vspeed_edges = processed_collection.find({
    "time": {"$gte": start_time, "$lte": end_time},
    "vspeed": max_vspeed
})

# Εκτύπωση αποτελέσματος για τη μεγαλύτερη μέση ταχύτητα
seen_links = set()  # Σύνολο για την καταγραφή των εμφανισμένων ακμών
print("Ακμές με τη μεγαλύτερη μέση ταχύτητα:")
for edge in largest_vspeed_edges:
    link = edge['link']
    if link not in seen_links:
        seen_links.add(link)
        print(f"Ακμή: {link}, Μέση ταχύτητα: {edge['vspeed']}")

print("---------------------3------------------------")

# Query 3: Υπολογισμός της μεγαλύτερης διαδρομής
# Εύρεση της μέγιστης και ελάχιστης θέσης για κάθε ακμή
pipeline = [
    {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
    {"$group": {
        "_id": "$link",
        "max_position": {"$max": "$position"},
        "min_position": {"$min": "$position"}
    }},
    {"$project": {
        "link": "$_id",
        "max_position": 1,
        "min_position": 1,
        "distance": {"$subtract": ["$max_position", "$min_position"]}
    }},
    {"$sort": {"distance": -1}}
]

largest_distance_edge = raw_collection.aggregate(pipeline).next()

# Εκτύπωση αποτελέσματος για τη μεγαλύτερη διαδρομή
print(f"Ακμή με τη μεγαλύτερη διαδρομή: {largest_distance_edge['link']}, Διαδρομή: {largest_distance_edge['distance']} μέτρα")

# Κλείσιμο της σύνδεσης με τη MongoDB
client.close()