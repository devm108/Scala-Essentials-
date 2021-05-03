var rate = 10.50
def payroll (s: String, h: Double): String = {
if (s!="y"){
if (h <= 40)
"Salary is: $" + h*rate
else { "Salary is: $" + (40*rate + (h-40)*rate*1.5)}
}
else "This is a salaried employee"}

println(payroll("n", 40)
println(payroll("n", 45)
println(payroll("y", 40)
