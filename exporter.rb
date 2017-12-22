require 'redis'
require 'csv'
require 'pry'

# r = Redis.new(db: 2)

# hash = {}
# r.scan(0, count: 100_1000, match: '*detail*')[1].each do |key|
#   info = r.hgetall(key)
#   if hash[info["Name"]].nil?
#     hash[info["Name"]] = {count: 1, row: info}
#   else
#     hash[info["Name"]][:count] += 1
#   end
# end

# hash.each do |k,v|
#   p v
# end

# CSV.open('./result3.csv', 'w') do |csv|
#   hash.each do |k, v|
#     row = v[:row]
#     csv << [
#       row["Name"],
#       row["Address"],
#       row["Business"],
#       row["Phone"],
#       v[:count]
#     ]
#   end
# end
hash = {}
Dir['*.csv'].each do |file|
  lines = CSV.read(file)
  lines.each do |l|
    if hash[l[0]].nil?
      hash[l[0]] = { count: l[4].to_i, row: l[0..3] }
    else
      hash[l[0]][:count] += 1
    end
  end
end
CSV.open('townwork.net.csv', 'w') do |csv|
  hash.each do |k, v|
    csv << [
      v[:row][0],
      v[:row][1],
      v[:row][2],
      v[:row][3],
      v[:count]
    ]
  end
end
