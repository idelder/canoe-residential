n_demands = length(Demandspecificdistributionsbyenduse.Properties.VariableNames);
DSDs = Demandspecificdistributionsbyenduse.Variables;
demands = Demandspecificdistributionsbyenduse.Properties.VariableNames;

% season, time of day, demand name, dsd
DSD_table = strings(8760*n_demands,5);

for dem = 1:n_demands
    for day = 1:365
        for hour = 1:24
            
            season = "D";
            if (day < 10)
                season = season + "00";
            elseif (day < 100)
                season = season + "0";
            end
            season = season + day;

            time_of_day = "H";
            if (hour < 10)
                time_of_day = time_of_day + "0";
            end
            time_of_day = time_of_day + hour;
            
            t = (day-1)*24 + hour;
            row = (dem-1)*8760 + t;
            
            DSD_table(row, :) = ["ON", season, time_of_day, demands(dem), DSDs(t,dem)];

        end
    end
end