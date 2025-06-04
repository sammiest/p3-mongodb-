// Query 5
// Find the oldest friend for each user who has a friend. For simplicity,
// use only year of birth to determine age, if there is a tie, use the
// one with smallest user_id. You may find query 2 and query 3 helpful.
// You can create selections if you want. Do not modify users collection.
// Return a javascript object : key is the user_id and the value is the oldest_friend id.
// You should return something like this (order does not matter):
// {user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname) {
    db = db.getSiblingDB(dbname);

    db.flat_users.drop();

    db.users.aggregate([
        { $unwind: "$friends" },
        { $project: { _id: 0, user_id: 1, friends: 1 } },
        { $out: "flat_users" }
    ]);

    let results = {};

    db.users.find({}, { user_id: 1 }).forEach(function(userDoc) {
        const uid = userDoc.user_id;
        const friendsList = [];

        db.flat_users.find({
            $or: [
                { user_id: uid },
                { friends: uid }
            ]
        }).forEach(function(pairDoc) {
            if (pairDoc.user_id === uid) {
                friendsList.push(pairDoc.friends);
            } 
            else {
                friendsList.push(pairDoc.user_id);
            }
        });

        if (friendsList.length === 0) return;; 

        const oldestCursor = db.users.find({ user_id: { $in: friendsList } }, { user_id: 1, YOB: 1 }).sort({ YOB: 1, user_id: 1 }).limit(1);

        if (oldestCursor.hasNext()) {
            const oldestDoc = oldestCursor.next();
            results[uid] = oldestDoc.user_id;
        }
    });

    return results;

}
