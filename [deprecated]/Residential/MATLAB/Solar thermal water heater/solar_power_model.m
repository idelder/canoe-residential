% vertical column vectors of irradiance for 8760 hours of the year
importdata("ON_solar_aggregate.mat");

% DNI (W/m2)
% DHI (W/m2)

epsilon = 1E-4; % threshold to set CF as zero (= 0; to disable)

latitude = 43.6532; % latitude at location +N -S
longitude = 79.3832; % longitude at location +W -E
delta_UTC = -5; % difference of timezone from UTC

area = 1; % m^2 total area of PV panels
az_panels = 0; % panel facing azimuth, degrees west from due south
tilt = latitude; % tilt of PV panels in degrees from horizontal
efficiency = 60/100; % photovoltaic efficiency of panels in sunlight

hour_days = ((1:365*24)./24)'; % days of the year passed for each hour, 0 to 365
daily_hours = 24*mod(hour_days,1); % hours through each day, 0 to 24
dec_angle = -23.45*cosd((360/365)*(hour_days+10)); % declination angle of the sun

% correcting for solar time
LSTM = 15*-delta_UTC; % local standard time meridian
B = (360/365)*(hour_days-81); % days from equinox
EOT = 9.87*sind(2*B) - 7.53*cosd(B) - 1.5*sind(B); % equation of time
time_correction = 4*(longitude - LSTM) + EOT; % solar time correction minutes
LST = daily_hours + time_correction/60; % local solar time
hour_angle = 15*(LST - 12); % hour angle, adjustes solar azimuth

% angle of the sun in the sky by hour
solar_alt = asind(cosd(latitude).*cosd(dec_angle).*cosd(hour_angle) + sind(latitude).*sind(dec_angle));
solar_az = acosd( (sind(dec_angle) - sind(solar_alt)*sind(latitude)) ./ (cosd(solar_alt).*cosd(latitude)) );
solar_az = (solar_az - 180)/2 .* (daily_hours<12) + (180 - solar_az)/2 .* (daily_hours>=12);
surface_az = solar_az - az_panels;
sun_surface_angle = acosd(cosd(solar_alt).*cosd(surface_az).*sind(tilt) + sind(solar_alt).*cosd(tilt));
sun_surface_angle(sun_surface_angle > 90 | sun_surface_angle < -90) = 90;

% direct irradiance on the panels
dir_I = DNI.* cosd(sun_surface_angle);
dir_I = dir_I.*(dir_I>0);

% diffuse irradiance on the panels
diff_I = DHI.*(180-tilt)/180;

hourly_irradiance = diff_I + dir_I;
hourly_cap_fact = hourly_irradiance * efficiency / 1360;
hourly_cap_fact = hourly_cap_fact .* (hourly_cap_fact > epsilon);

plot(hourly_cap_fact(12:24:8760))