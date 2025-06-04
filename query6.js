// Query 6
// Find the average friend count per user.
// Return a decimal value as the average user friend count of all users in the users collection.

function find_average_friendcount(dbname) {
    db = db.getSiblingDB(dbname);

    const totalUsers = db.users.count();

    if (totalUsers === 0) return 0;

    let sumFriends = 0;
    db.users.find({}, { friends: 1 }).forEach(function (u) {
        if (Array.isArray(u.friends)) {
            sumFriends += u.friends.length;
        }
    });

    return sumFriends / totalUsers;
}
