class ModuleAlarm
{
	public static void main(String[] args) 
	{
		StringBuilder sbl=new StringBuilder("jagan");
		System.out.println(sbl);
		sbl.append(" added in string builder");//append 
		System.out.println(sbl);
		sbl.insert(0,'A');//insert 
		System.out.println(sbl);
		System.out.println(sbl.charAt(4));
		sbl.delete(20,25);//delete
		System.out.println(sbl);
		sbl.reverse();//reverse
		System.out.println(sbl);
		System.out.println(sbl.capacity());
		System.out.println(sbl.length());
System.out.println("added new line1")

	}
}
