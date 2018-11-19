class ModuleResources
{
	void m1()
	{m3();//calling method 2 here it will execute m2 first then goes to m1 method
		System.out.println("m1");
		m3();//calling instance method inside another instance method 
	}
	
	 void m3() 	{
		System.out.println("m2 method ");
	}

	public static void main(String[] args) 
	{
		ModuleResources obj =new ModuleResources();
		obj.m1();//calling m1 method 
		
	}
}
