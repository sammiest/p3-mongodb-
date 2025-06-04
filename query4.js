// Query 4
// Find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// user_id is the field from the users collection. Do not use the _id field in users.
// Return an array of arrays.

function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);

    let pairs = [];
    const user_a = db.users.find({gender : "male"}).toArray();
    const user_b = db.users.find({gender : "female"}).toArray();

    user_a.forEach(a => {
        user_b.forEach(b => {
            if (a.hometown.city === b.hometown.city){
                const age_gap = Math.abs(a.YOB - b.YOB);
                if (age_gap < year_diff){
                    const a_friend_list = a.friends
                    const b_friend_list = b.friends

                    const are_friends = a_friend_list.includes(b.user_id) || b_friend_list.includes(a.user_id);
                    if (!are_friends) {
                        pairs.push([a.user_id, b.user_id]);
                    }
                }
            }
        });
    });

    return pairs;
}
