class ModuleMap
{
	int a=20;//instance
	static int a=30;//static 
	static double b=100.89;
	void m1()
	{
		int a=40;//local
		double b=111.11;
		System.out.println(a);
		System.out.println(a);
		System.out.println(b);
	}


	public static void main(String[] args) 
	{
		System.out.println("Checking for instance,local,static variable can be same or not");
		Vari1 obj1=new Vari1();
		obj1.m1();
	}
}
// instance variable and local variable can be same 
//static variable and local variable can be same 