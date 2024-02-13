close all;

efficient_demands = @(eff) ([zeros(5,15),eff*(0:30)])' + [end_use;ones(30,1)*end_use(16,:)];
fuel_model = @(X) efficient_demands(X(:,1))*X(:,2:5);

chi_squared_1 = @(X) sum((fuel_model(X)-projected_fuels).^2 ./ projected_fuels,"all");
chi_squared_2 = @(X) sum((sum(fuel_model(X), 2) - sum(projected_fuels,2)).^2 ./ sum(projected_fuels,2));
chi_squared_3 = @(X) sum( (efficient_demands(X(:,1)) - [end_use;ones(30,1)*end_use(16,:)]).^2 ./ [end_use;ones(30,1)*end_use(16,:)], "all");
obj = @(X) chi_squared_1(X) + chi_squared_2(X) + chi_squared_3(X);

opts = optimoptions("fmincon","MaxFunctionEvaluations",1E6);
coeffs = fmincon(obj,zeros(5,5),[],[],[],[],lb,ub,@sum_to_one,opts);

years = (2005:2020)';
projected_years = (2005:2050)';
modelled_fuels = fuel_model(coeffs);

end_uses = ["Space heating","Water heating","Appliances","Lighting","Space cooling"];
fuels = ["Natural gas","Electricity","Oil","Biofuels & emerging"];

for i = 1:4
    figure()
    plot(projected_years, modelled_fuels(:,i), "r-", projected_years, projected_fuels(:,i), "b-");
    title(fuels(i));
end

figure()
plot(projected_years, sum(modelled_fuels,2), "r-", years, sum(end_use,2), "b-", projected_years, sum(projected_fuels,2), "k-")

coeff_table = ["","Temporal",fuels;end_uses',coeffs];
% projected_es_table = ["",titles;projected_years(16:5:length(modelled_fuels),:),projected_end_use];

efficient_demands = @(D) (coeffs(:,1)*(0:45))' + D;
fuel_model = @(D) efficient_demands(D)*coeffs(:,2:5);

chi_squared_1 = @(D) sum(((fuel_model(D)-projected_fuels).^2 ./ projected_fuels),"all");
chi_squared_2 = @(D) sum((sum(fuel_model(D), 2) - sum(projected_fuels,2)).^2 ./ sum(projected_fuels,2));
chi_squared_3 = @(D) sum(((D(1:16,:) - end_use).^2 ./ end_use), "all");
obj = @(D) chi_squared_1(D) + chi_squared_2(D) + chi_squared_3(D);

modelled_demands = ([zeros(5,15),coeffs(:,1)*(0:30)])' + [end_use;ones(30,1)*end_use(16,:)];

for i = 1:5
    figure()
    plot(projected_years, modelled_demands(:,i), "r-", years, end_use(:,i), "b-");
    title(end_uses(i));
end

figure()
plot(projected_years, sum(modelled_demands,2), "r-", years, sum(end_use,2), "b-", projected_years, sum(projected_fuels,2), "k-")
