#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <array>
#include <thread>
#include <iomanip>
#include <cctype>
#include <algorithm>
#include <utility>


using namespace std;

const string FILE_NAME ="SPY_May_2012.csv";
const string OUTPUT_FILE = "outputs.csv";

const int NUM_PORTIONS = 26;
const int NUM_INTERVALS = 27;
const int NUM_TRADING_DAYS = 14;

string trim(const string& s){
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

vector<string> splitCSV(const string& line) {
    vector<string> out;
    string token;
    stringstream ss(line);
    while (getline(ss, token, ',')) {
        out.push_back(trim(token));
    }
    return out;
}

void convertToNY(int gmtH, int gmtM, int offset, int& nyH, int& nyM){
    int totalMin = (gmtH + offset) * 60 + gmtM;
    if (totalMin < 0){
        totalMin += 1440;
    }
    else if (totalMin >= 1440){
        totalMin -= 1440;
    }
    nyH = totalMin / 60;
    nyM = totalMin % 60;
}

// 0 - 26
int intervalIndex(int h, int m){
    int totalMin = h * 60 + m;
    int startMin = 9 * 60 + 30;

    int diff = totalMin - startMin;
    if (diff < 0 || diff >= 15 * NUM_INTERVALS) {
        return -1;
    }

    return diff / 15;
}
struct MapValue 
{
    bool hasQty = false;
    long long qty = 0;
    bool hasPrice = false;
    double price = 0.0;
    string gmtTimeStr;
};

vector<vector<pair<int, MapValue>>> mapperOutputs(NUM_PORTIONS);

struct IntervalAgg{
    long long qtySum = 0;
    double price521 = 0.0;
    string earlistTime;
    bool hasPrice = false;
};


void mapTask(int mapID){
    ifstream fin(FILE_NAME);
    if (!fin.is_open()){
        cerr << "Cannot open input file: " << FILE_NAME << endl;
        return;
    }
    string line;

    if (!getline(fin, line)){
        cerr << "Empty file." << endl;
        return;
    }

    long long lineNo = 0;
    auto& out = mapperOutputs[mapID];

    while (getline(fin, line)){
        ++lineNo;
        if ((lineNo - 1) % NUM_PORTIONS != mapID) continue;
        if (line.empty()) continue;

        auto cols = splitCSV(line);
        if (cols.size() < 7) continue;

        string date = cols[1];
        string timestamp = cols[2];
        string offsetStr = cols[3];
        string type = cols[4];

        if (type != "Trade") continue;

        double price = 0.0;
        long long qty = 0;
        int offset = 0;

        try {
            price = stod(cols[5]);
            qty = stoll(cols[6]);
            offset = stoi(offsetStr);
        }
        catch (...){
            continue;
        }
        if (date.size() < 2) continue;
        int day = 0;
        try {
            day = stoi(date.substr(0, 2));
        } catch (...){
            continue;
        }
        if (day < 1 || day > 21) continue;

        if (timestamp.size() < 5) continue;
        int gmtH = 0;
        int gmtM = 0;
        try {
            gmtH = stoi(timestamp.substr(0, 2));
            gmtM = stoi(timestamp.substr(3, 2));
        } catch (...){
            continue;
        }

        int nyH = 0;
        int nyM = 0;
        convertToNY(gmtH, gmtM, offset, nyH, nyM);
        int interval = intervalIndex(nyH, nyM);

        if (interval < 0 || interval >= NUM_INTERVALS) continue;

        MapValue mv;
        if (day >= 1 && day <= 20){
            mv.hasQty = true;
            mv.qty = qty;
        }

        if (day == 21){
            mv.hasPrice = true;
            mv.price = price;
            mv.gmtTimeStr = timestamp;
        }

        if (mv.hasQty || mv.hasPrice){
            out.emplace_back(interval, mv);
        }
    }

    fin.close();
}

int main(){
    vector<thread> MapThreads;
    for (int i = 0; i < NUM_PORTIONS; ++i){
        MapThreads.emplace_back(mapTask, i);
    }
    for (auto& t : MapThreads){
        t.join();
    }

    // shuffling/reducing
    array<vector<MapValue>, NUM_INTERVALS> intervalBuckets;
    for (int i = 0; i < NUM_PORTIONS; ++i){
        for(const auto& kv : mapperOutputs[i]){
            int interval = kv.first;
            if (interval < 0 || interval >= NUM_INTERVALS) continue;
            intervalBuckets[interval].push_back(kv.second);
        }
        mapperOutputs[i].clear();
        mapperOutputs[i].shrink_to_fit();
    }

    array<IntervalAgg, NUM_INTERVALS> finalResults;

    for (int iv = 0; iv < NUM_INTERVALS; ++iv){
        auto& agg = finalResults[iv];

        for (const auto& mv : intervalBuckets[iv]){
            if (mv.hasQty){
                agg.qtySum += mv.qty;
            }
            if (mv.hasPrice){
                if (!agg.hasPrice || mv.gmtTimeStr < agg.earlistTime){
                    agg.price521 = mv.price;
                    agg.earlistTime = mv.gmtTimeStr;
                    agg.hasPrice = true;
                }
            }
        }
    }

    ofstream fout(OUTPUT_FILE);
    if (!fout.is_open()){
        cerr << "Cannot open output file: " << OUTPUT_FILE << endl;
        return 1;
    }

    fout << "Time Interval (NY Local Time),"
         << "Quantity (Avg Qty from 5/1-5/20),"
         << "Price (Trading Price from 5/21)\n";

    int baseH = 9;
    int baseM = 30;

    for (int i = 0; i < NUM_INTERVALS; ++i){
        int startMin = baseM + i * 15;
        int sh = baseH + startMin / 60;
        int sm = startMin % 60;

        ostringstream tss;
        tss << setfill('0') << setw(2) << sh << ":" << setfill('0') << setw(2) << sm << ":00";
        string timeStr = tss.str();

        long long avgQty = static_cast<long long>((double)finalResults[i].qtySum / NUM_TRADING_DAYS + 0.5);
        fout << timeStr << "," << avgQty << "," << fixed << setprecision(2) << finalResults[i].price521 << "\n";
    }
    fout.close();
    cout << "Processing completed. Output written to " << OUTPUT_FILE << endl;
    return 0;
}