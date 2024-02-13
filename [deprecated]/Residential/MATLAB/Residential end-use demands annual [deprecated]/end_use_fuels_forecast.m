close all;

fuel_by_demand_model = @(x) x*years;
sum_fuels = @(data) [sum(data(1:5:21,:),1);sum(data(2:5:22,:),1);sum(data(3:5:23,:),1);sum(data(4:5:24,:),1)];
sum_uses = @(data) [sum(data(1:5,:),1);sum(data(6:10,:),1);sum(data(11:15,:),1);sum(data(16:20,:),1);sum(data(21:25,:),1)];

chi_squared = @(model,data) sum((model - data).^2 ./ max(abs(data),0.001) , "all");

obj_fuels_by_demand = @(x) chi_squared(fuel_by_demand_model(x), annual_fuels_by_demand);
obj_sum_fuels = @(x) chi_squared(sum_fuels(fuel_by_demand_model(x)), energy_by_fuel);
obj_sum_uses = @(x) chi_squared(sum_uses(fuel_by_demand_model(x)), energy_by_end_use);

objective = @(x) obj_fuels_by_demand(x) + obj_sum_fuels(x) + obj_sum_uses(x);

opts = optimoptions("fmincon", MaxFunctionEvaluations=1E4);
x = fmincon(objective, x0, [], [], [], [], lb, ub, [], opts);

modelled = fuel_by_demand_model(x);
base_years = 2000:2018;

projected_years = [0:50;ones(1,51)];
projected = x*projected_years;

figure(1)
for i = 1:4
    plot(base_years, energy_by_fuel, "b-", base_years, sum_fuels(modelled), "r-")
end
title("Fuel demands")

figure(2)
for i = 1:4
    plot(base_years, energy_by_end_use, "b-", base_years, sum_uses(modelled), "r-")
end
title("End use demands")

figure(3); hold on;
plot(2005:2050, projected_energy_demand, 'k-');
plot(2000:2050, sum(projected, 1), 'r-');
plot(base_years, sum(annual_fuels_by_demand, 1), 'b-');
title("Total energy demand")

projected_uses = sum_uses(projected);
forecast_demands = [["","Space heating","Water heating","Appliances","Lighting","Cooling"]',...
    [2025:5:2050;projected_uses(:,26:5:51)]];