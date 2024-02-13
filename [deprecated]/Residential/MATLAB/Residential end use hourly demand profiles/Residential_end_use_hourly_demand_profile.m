% Ian David Elder
% Hourly demand profiles for residential end use demands Ontario
% 1. Download 15-minute time-series data for a whole year and all uses
% 2. Open the csv and save it again as an Excel workbook
% 3. Open it in MATLAB and import as a string array (including headers)

load('supporting_data.mat')
load('15minutetimeseriesdata.mat')

%% Strip 15 minute demands to end uses
otherapp_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),otherapp_uses));
otherapp_demands = otherapp_demands(2:size(otherapp_demands,1),:);
otherapp_demands = str2double(otherapp_demands);

refrigerator_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),refrigerator_uses));
refrigerator_demands = refrigerator_demands(2:size(refrigerator_demands,1),:);
refrigerator_demands = str2double(refrigerator_demands);

freezer_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),freezer_uses));
freezer_demands = freezer_demands(2:size(freezer_demands,1),:);
freezer_demands = str2double(freezer_demands);

dryer_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),dryer_uses));
dryer_demands = dryer_demands(2:size(dryer_demands,1),:);
dryer_demands = str2double(dryer_demands);

clotheswasher_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),clotheswasher_uses));
clotheswasher_demands = clotheswasher_demands(2:size(clotheswasher_demands,1),:);
clotheswasher_demands = str2double(clotheswasher_demands);

dishwasher_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),dishwasher_uses));
dishwasher_demands = dishwasher_demands(2:size(dishwasher_demands,1),:);
dishwasher_demands = str2double(dishwasher_demands);

lighting_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),lighting_uses));
lighting_demands = lighting_demands(2:size(lighting_demands,1),:);
lighting_demands = str2double(lighting_demands);

waterheating_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),waterheating_uses));
waterheating_demands = waterheating_demands(2:size(waterheating_demands,1),:);
waterheating_demands = str2double(waterheating_demands);

spaceheating_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),spaceheating_uses));
spaceheating_demands = spaceheating_demands(2:size(spaceheating_demands,1),:);
spaceheating_demands = str2double(spaceheating_demands);

spacecooling_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),spacecooling_uses));
spacecooling_demands = spacecooling_demands(2:size(spacecooling_demands,1),:);
spacecooling_demands = str2double(spacecooling_demands);

cooking_demands = minutetimeseriesdata(:,contains(minutetimeseriesdata(1,:),cooking_uses));
cooking_demands = cooking_demands(2:size(cooking_demands,1),:);
cooking_demands = str2double(cooking_demands);

%% Sum all uses within each end use category
demands_15m = [...
    sum(spaceheating_demands,2),...
    sum(spacecooling_demands,2),...
    sum(waterheating_demands,2),...
    sum(lighting_demands,2),...
    sum(cooking_demands,2),...
    sum(refrigerator_demands,2),...
    sum(freezer_demands,2),...
    sum(dishwasher_demands,2),...
    sum(clotheswasher_demands,2),...
    sum(dryer_demands,2),...
    sum(otherapp_demands,2)...
    ];

n_demands = size(demands_15m,2);

demands_8760 = zeros(8760,n_demands);
DSDs = zeros(8760,n_demands);
DSD_table = zeros(8760*n_demands,1);

for d = 1:n_demands
    for h = 1:8760
        demands_8760(h,:) = demands_15m(4*h,:);
    end

    DSDs(:,d) = demands_8760(:,d)/sum(demands_8760(:,d));
    DSDs(DSDs(:,d) < 1E-7, d) = 0;
end

%% Plot to check
close all;

for d = 1:n_demands
    plot(DSDs(:,d)); hold on;
end