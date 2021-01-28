using CSV
using DataFrames
using Dates
using ShiftedArrays
using GLM

using StatsBase
using Statistics

df = CSV.read("bloomberg_index.csv", DataFrame)
hy_df = df[:,[:Date, :US_High_Yield]]
dropmissing!(hy_df)
hy_df[!,"lag"] = mapcols(lag, hy_df)[!,"US_High_Yield"]
num_df = hy_df[:,["US_High_Yield","lag"]]
mapcols!(x -> log.(x), num_df)
hy_df[!,"return"] = num_df[!,"US_High_Yield"] - num_df[!,"lag"]
hy_df.Date = Date.(hy_df.Date,"y/m/d")

factor_df = CSV.read("FI400L_FacRet.20210115", DataFrame, delim="|")
factor_df[:,"Date"] = string.(factor_df[:,"DataDate"])
factor_set = Set{String}(factor_df[!,:Factor])

da = "df_test = DataFrame(Date=String[],"
for item in factor_set
    global da
    da *= (item * "=Float64[],")
end
da *= ")"
eval(Meta.parse(da))

date_set = Set{String}(factor_df[!,:Date])
date_array = []
for item in date_set
    push!(date_array,item)
end

for item in date_set
    push!(df_test, Dict(:Date=>item), cols=:union)
end

i = 1
for i in 1:length(factor_df[!,:Date])
    df_test[df_test.Date.==factor_df[i,:Date],factor_df[i,:Factor]] .= factor_df[i,:Return]
end

df_test.Date = Date.(df_test.Date,"yyyymmdd")
sort!(df_test,:Date)
data_set = innerjoin(df_test,hy_df[!,["Date","return"]],on=:Date)
data_set |> CSV.write("data_set.csv",delim=',',writeheader=true)

us_factor_list = String[]
for s in factor_set
    if occursin(r"^US", s) 
        push!(us_factor_list,s)
    end
end

us_factor_list_x = copy(us_factor_list)
push!(us_factor_list,"Date")
push!(us_factor_list,"return")
us_data_set = data_set[!,us_factor_list]

dropmissing(us_data_set) |> CSV.write("us_data_set_only.csv",delim=',',writeheader=true)
dropmissing!(us_data_set)

rename!(us_data_set, :return => :return_log)
x_string = join(us_factor_list_x, " + ")
go = "ols = lm(@formula(return_log ~ " * x_string * "),us_data_set, true)"
eval(Meta.parse(go))