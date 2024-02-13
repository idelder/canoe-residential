function [c,ceq] = sum_to_one(X)
c = [];
ceq = X(:,2:5)*ones(4,1) - ones(5,1);